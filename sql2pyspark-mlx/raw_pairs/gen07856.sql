SELECT p.brand, COUNT(*) AS cnt, MAX(src.revenue) AS max_revenue FROM store_sales src JOIN products p ON src.product_id = p.id GROUP BY ROLLUP (p.brand)
