SELECT c.segment, SUM(o.discount) AS total_discount
FROM orders o
LEFT OUTER JOIN stores s ON s.id = o.store_id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE s.city = 'Tokyo'
GROUP BY c.segment
