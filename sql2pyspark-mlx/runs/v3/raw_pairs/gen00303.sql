SELECT
    p.amount,
    o.store_id,
    c.credit_limit,
    TRIM(o.status) clean_status
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
INNER JOIN customers c ON o.customer_id = c.id;
