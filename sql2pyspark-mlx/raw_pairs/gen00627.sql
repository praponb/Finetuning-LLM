SELECT o.status, SUM(CASE WHEN p.status = 'FAILED' THEN p.amount ELSE 0 END) failed_amount, COUNT(p.amount) amount_count
FROM payments p
JOIN orders o ON o.id = p.order_id
WHERE o.discount > 0.39
GROUP BY o.status
ORDER BY failed_amount;
