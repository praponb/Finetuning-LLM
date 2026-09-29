SELECT signup_date FROM customers UNION ALL SELECT sale_date AS signup_date FROM store_sales WHERE product_id IN (3, 4, 5, 9) ORDER BY signup_date;
