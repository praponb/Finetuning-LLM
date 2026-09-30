SELECT
    p.amount,
    p.status,
    o.customer_id,
    o.discount
FROM payments p
LEFT OUTER JOIN orders o ON o.id = p.order_id
WHERE o.customer_id IN (11, 34) AND p.status NOT IN ('FAILED', 'SUCCESS', 'REFUNDED')
ORDER BY customer_id DESC, p.status
