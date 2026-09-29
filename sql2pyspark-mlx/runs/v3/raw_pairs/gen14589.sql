SELECT
    p.id,
    p.status,
    o.order_date,
    o.status AS order_status,
    s.region,
    s.city
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON s.id = o.store_id
WHERE p.method IN ('bank_transfer', 'promptpay') OR s.opened_date < '2025-02-17';
