SELECT
    p.id,
    p.status,
    p.paid_at,
    o.customer_id,
    o.store_id,
    c.credit_limit
FROM payments p
FULL OUTER JOIN orders o ON o.id = p.order_id
JOIN customers c ON c.id = o.customer_id
WHERE c.country IN ('JP', 'UK', 'TH') OR o.status IN ('CANCELLED', 'RETURNED');
