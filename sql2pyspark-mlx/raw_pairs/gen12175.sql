SELECT s.id, s.country, COUNT(p.id) AS product_count
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
WHERE s.rating <= 2.1
GROUP BY s.id, s.country
HAVING COUNT(p.id) > 3
ORDER BY 3 DESC
