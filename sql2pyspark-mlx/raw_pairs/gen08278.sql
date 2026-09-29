SELECT o.status, COUNT(*) AS row_count
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
FULL OUTER JOIN stores s ON o.store_id = s.id
GROUP BY o.status
ORDER BY o.status, row_count DESC;
