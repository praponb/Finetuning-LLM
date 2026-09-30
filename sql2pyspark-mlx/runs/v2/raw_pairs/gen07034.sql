SELECT p.category, p.brand, MAX(ss.units) - MIN(ss.units) AS units_range FROM store_sales ss RIGHT JOIN products p ON ss.product_id = p.id GROUP BY p.category, p.brand
