SELECT p.category, SUM(ss.revenue) AS total_revenue FROM store_sales ss JOIN products p ON p.id = ss.product_id JOIN suppliers s ON s.id = p.supplier_id GROUP BY p.category;
