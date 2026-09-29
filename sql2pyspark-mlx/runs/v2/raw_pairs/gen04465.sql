SELECT c.segment, SUM(CASE WHEN p.method = 'bank_transfer' THEN 1 ELSE 0 END) AS bank_transfer_count, ROUND(AVG(p.amount), 2) AS avg_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.segment
HAVING ROUND(AVG(p.amount), 2) >= 42
