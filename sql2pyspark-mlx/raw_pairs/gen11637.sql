SELECT
    p.amount,
    p.method,
    o.order_date,
    o.customer_id
FROM payments p
FULL OUTER JOIN orders o ON o.id = p.order_id
WHERE p.status IN ('SUCCESS', 'FAILED', 'REFUNDED') AND p.amount > 510
