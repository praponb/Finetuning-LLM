SELECT p.category, MAX(ss.sale_date) latest_sale_date, COUNT(DISTINCT ss.revenue) unique_stores
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
GROUP BY p.category
