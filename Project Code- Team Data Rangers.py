
# coding: utf-8

# In[1]:


#Loading the Data
import pandas as pd
aisles = pd.read_csv('C:/Users/Kunal/Desktop/mohita/Project/aisles.csv')
departments = pd.read_csv('C:/Users/Kunal/Desktop/mohita/Project/departments.csv')
order_products_prior = pd.read_csv('C:/Users/Kunal/Desktop/mohita/Project/order_products_prior.csv')
order_products_train = pd.read_csv('C:/Users/Kunal/Desktop/mohita/Project/order_products__train.csv')
orders = pd.read_csv('C:/Users/Kunal/Desktop/mohita/Project/orders.csv')
products = pd.read_csv('C:/Users/Kunal/Desktop/mohita/Project/products.csv')
sample_submission = pd.read_csv('C:/Users/Kunal/Desktop/mohita/Project/sample_submission.csv')


# In[4]:


import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
color = sns.color_palette()

get_ipython().run_line_magic('matplotlib', 'inline')


# In[5]:


plt.figure(figsize=(12,8))
sns.countplot(x="days_since_prior_order", data=orders, color=color[3])
plt.ylabel('Count', fontsize=12)
plt.xlabel('Days since prior order', fontsize=12)
plt.xticks(rotation='vertical')
plt.title("Frequency distribution by days since prior order", fontsize=15)
plt.show()


# In[11]:


order_products_train_df = pd.merge(order_products_train, orders, on='order_id', how='left')
grouped_df = order_products_train_df.groupby(["order_dow"])["reordered"].aggregate("mean").reset_index()


# In[14]:


grouped_df = order_products_train_df.groupby(["order_dow", "order_hour_of_day"])["reordered"].aggregate("mean").reset_index()
grouped_df = grouped_df.pivot('order_dow', 'order_hour_of_day', 'reordered')
plt.figure(figsize=(12,6))
sns.heatmap(grouped_df)
plt.title("Reorder ratio of Day of week Vs Hour of day")
plt.show()


# In[ ]:


order_products_prior_df = pd.merge(order_products_prior, products_df, on='product_id', how='left')
order_products_prior_df = pd.merge(order_products_prior_df, aisles_df, on='aisle_id', how='left')
order_products_prior_df = pd.merge(order_products_prior_df, departments_df, on='department_id', how='left')
order_products_prior_df.head()


# In[2]:


#Merging relevant datasets
result = [order_products_prior,order_products_train]
#result
order_products_all = pd.concat(result)
order_products_all
merge_data_orders=pd.merge(order_products_all,orders,on="order_id",how="left").sort_values(by='user_id')
merge_data_orders=pd.merge(merge_data_orders,products,on="product_id",how="left")
merge_data_orders['order_dow'].replace([0,1,2,3,4,5,6],['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],inplace=True)
merge_data_orders.head()


# In[3]:


#Maximum sold product from market basket; selecting top 10 products only
trial = merge_data_orders.groupby(['product_name'],as_index=False)['product_id'].count()
trial=trial.sort_values('product_id',ascending=False)
Max_sold_product=pd.merge(trial,products,on="product_name",how="right")
del Max_sold_product['aisle_id']
Max_sold_product.columns=['Product_name','Total_sales_qty','Product_id','Department_id']
Max_sold_product=Max_sold_product.head(10)
Max_sold_product['Total_sales_qty']=Max_sold_product['Total_sales_qty'].astype('int64')
Max_sold_product


# In[5]:


#Peak hours of the day by counting products ordered at that time
order_time=merge_data_orders['order_hour_of_day'].value_counts().to_frame()
order_time['hour_of_day']=order_time.index
order_time.columns=['count','hour_of_day']
order_time=order_time.head(10)
order_time


# In[6]:


#Top 5 products by total sales and how much percentage on which day 
trial.columns=['Product_name','Total_sales_qty']
trial=trial.head(5)
trial["Day_of_week"]=""
trial["Max_sold_day"]=""

for index, row in trial.iterrows():
    y=merge_data_orders[merge_data_orders['product_name']==row['Product_name']].groupby('order_dow')['product_id'].count()
    y=y.sort_values(ascending=False)
    trial.at[index, 'Max_sold_day']=y[0]
    trial.at[index, 'Day_of_week']=y.index[0]

for index, row in trial.iterrows():
    trial.at[index, 'Percentage']=(row['Max_sold_day']/row['Total_sales_qty'])*100

trial['Percentage']=trial['Percentage'].round(2)
trial


# In[7]:


#locate top 5 users with maximum qty purchase
  users=merge_data_orders.groupby('user_id')['product_id'].count().sort_values(ascending=False)
users=users.head(5).to_frame()

users['User_id']=users.index
users=users.rename(columns={'product_id':'Total_units_buy'})
users


# In[13]:


#For top 5 users, which department they maximum purchased from and what qty
topdept=pd.DataFrame()
for x in users['User_id']:
    y=merge_data_orders[merge_data_orders['user_id']==x].groupby(['department_id'],as_index=False)['product_id'].count()
    y=y.sort_values(by='product_id',ascending=False)
    y=y.head(1)
    y['User_id']=[x]
    topdept=topdept.append(y,ignore_index=True)
topdept['User_id']=topdept['User_id'].astype('int64')
topdept=topdept[['User_id','department_id','product_id']]
topdept.columns=['User_id','Department_id','Departmental_buy']
topdept


# In[14]:


# reorganize and clean the above table to find out what % of total purchase of individual top 5 users
# This % is of the max departmental purchase and reflects from which dept
top_users=pd.merge(topdept,users,on="User_id",how='right')
top_users.columns=['User_id','Department_id','Departmental_buy','Total_units_buy']
top_users["Percentage_buy"]=((top_users['Departmental_buy']/top_users['Total_units_buy'])*100).round(2)
top_users


# In[15]:



order_week=orders['order_dow'].value_counts().to_frame()
order_week['day_of_week']=order_week.index
order_week.columns=['count','day_of_week']
order_week.sort_values('count',ascending=False)
order_week['day_of_week'].replace([0,1,2,3,4,5,6],['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],inplace=True)
order_week


# In[17]:


# Recommend 1: Recommendation based on the historical purchases of this user(top user):
histrec=merge_data_orders[merge_data_orders['user_id']==201268]
histrec=histrec[histrec['department_id']==16].groupby(['user_id','product_name'],as_index=False)['product_id'].count().sort_values(by='product_id',ascending=False).head(10)
histrec.columns=['User_id','Product_name','Count']
histrec


# In[18]:


# Recommend 2: Recommendation of similar departmental products based on the their choice from same top dept (content based recommendation):
futrec=merge_data_orders[merge_data_orders['user_id']==201268]
futrec=futrec[futrec['department_id']==16].groupby(['user_id','product_name'],as_index=False)['product_id'].count().sort_values(by='product_id',ascending=False).tail(10)
futrec.columns=['User_id','Product_name','Count']
futrec


# In[19]:


# Recommend 3:Recommendation based on direct exit to cart checkout:
top_3=topdept['User_id'].head(3)
cartrec=pd.DataFrame()
for x in top_3:
    j=merge_data_orders[merge_data_orders['user_id']==x].groupby(['department_id'],as_index=False)['product_id'].count()
    j=j.sort_values(by='product_id',ascending=False)
    j=j.head(3)
    for y in j['department_id']:
        prod_3=merge_data_orders[merge_data_orders['user_id']==x]
        prod_3=prod_3[prod_3['department_id']==y].groupby(['user_id','product_name'],as_index=False)['product_id'].count().sort_values(by='product_id',ascending=False).head(3)
        prod_3["Department_id"]=[y,y,y]
        cartrec=cartrec.append(prod_3,ignore_index=True)
        
cartrec

