SELECT
    p.id,
    p.paid_at,
    o.order_date,
    o.customer_id,
    s.opened_date,
    s.region
FROM payments p
JOIN orders o ON p.order_id = o.id
FULL OUTER JOIN stores s ON o.store_id = s.id
ORDER BY o.customer_id;
