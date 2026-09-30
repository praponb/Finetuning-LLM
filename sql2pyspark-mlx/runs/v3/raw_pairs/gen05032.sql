SELECT id, product_id, sale_date, units, units - LAG(units, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS units_change FROM store_sales ORDER BY sale_date DESC;
