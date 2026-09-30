SELECT
    p.order_id,
    p.method,
    o.customer_id,
    o.order_date,
    o.discount,
    UPPER(o.status) AS status_upper
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
