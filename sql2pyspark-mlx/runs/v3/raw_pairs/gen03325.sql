SELECT id, product_id, units, ROUND(units / SUM(units) OVER (PARTITION BY product_id), 3) AS units_share FROM store_sales;
