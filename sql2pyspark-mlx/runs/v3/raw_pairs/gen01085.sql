SELECT o.status, SUM(CASE WHEN p.status = 'FAILED' THEN p.amount ELSE 0 END) failed_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
WHERE o.amount BETWEEN 520 AND 610
GROUP BY o.status
ORDER BY failed_amount ASC, status ASC
LIMIT 5
