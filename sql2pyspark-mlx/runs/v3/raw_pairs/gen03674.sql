SELECT
    c.is_active,
    c.credit_limit,
    c.country,
    o.customer_id,
    o.amount,
    o.store_id
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.customer_id IN (2, 6) AND c.segment IN ('SMB', 'Consumer', 'Corporate');
