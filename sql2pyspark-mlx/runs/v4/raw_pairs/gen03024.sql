SELECT p.category, SUM(ss.revenue) AS total_revenue, MAX(ss.sale_date) AS last_sale_date
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.stock_qty != 351
GROUP BY p.category
HAVING SUM(ss.revenue) > 22000
ORDER BY total_revenue;
