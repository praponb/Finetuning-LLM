SELECT id, product_id, ROUND(units / revenue, 2) AS revenue_per_unit FROM store_sales WHERE store_id IN (3, 4, 5, 6) AND (revenue < 1400 OR product_id IN (6, 10)) ORDER BY id
