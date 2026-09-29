SELECT p.brand, COUNT(*) AS num_rows, ROUND(AVG(a.units), 2) AS avg_units FROM store_sales a INNER JOIN products p ON a.product_id = p.id GROUP BY ROLLUP (p.brand) ORDER BY brand DESC;
