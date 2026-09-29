SELECT p.brand, MAX(ss.revenue) AS max_revenue
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY p.brand
ORDER BY max_revenue DESC, p.brand DESC;
