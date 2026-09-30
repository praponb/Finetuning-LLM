SELECT p.brand, COUNT(*) AS num_rows, MIN(s.revenue) AS min_revenue, MIN(s.units) AS min_units FROM store_sales s JOIN products p ON s.product_id = p.id GROUP BY ROLLUP (p.brand);
