SELECT id, revenue, product_id, LAST_DAY(sale_date) AS sale_date_month_end FROM store_sales WHERE NOT (product_id IN (1, 3, 6, 10) AND (sale_date <= '2024-09-01' OR store_id = 3));
