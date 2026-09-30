SELECT store_id, product_id, revenue, LAST_DAY(sale_date) AS sale_date_month_end FROM store_sales;
