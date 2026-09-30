SELECT
    p.status,
    p.paid_at,
    p.amount,
    o.channel,
    o.status AS order_status,
    CAST(p.paid_at AS DATE) AS paid_at_day
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
ORDER BY channel DESC;
