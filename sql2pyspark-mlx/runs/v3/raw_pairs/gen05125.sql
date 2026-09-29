SELECT id, sale_date, product_id, revenue, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date FROM store_sales WHERE units <> 35;
