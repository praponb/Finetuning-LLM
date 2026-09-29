SELECT id, store_id, revenue, units, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date FROM store_sales WHERE product_id IN (1, 3, 8, 10) AND revenue > 1700;
