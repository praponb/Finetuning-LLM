SELECT p.category, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue
FROM store_sales ss
RIGHT JOIN products p ON p.id = ss.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.cost < 630
GROUP BY p.category
HAVING stddev_revenue > 49
