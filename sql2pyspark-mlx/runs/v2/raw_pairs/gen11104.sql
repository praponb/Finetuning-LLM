SELECT id, store_id, sale_date, ROUND(units, 2) AS units_rounded FROM store_sales WHERE product_id IN (1, 3, 6) AND units IS NULL ORDER BY sale_date DESC, id ASC LIMIT 10;
