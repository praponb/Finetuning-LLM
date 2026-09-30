SELECT opened_date FROM stores WHERE region = 'North' EXCEPT SELECT sale_date FROM store_sales;
