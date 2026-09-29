SELECT
    o.status,
    COUNT(p.id) AS payment_count,
    MAX(p.amount) AS max_amount,
    COUNT(CASE WHEN p.method = 'cash' THEN 1 END) AS cash_count
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE o.store_id IS NOT NULL
GROUP BY o.status;
