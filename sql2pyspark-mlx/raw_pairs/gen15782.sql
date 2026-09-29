SELECT
    oi.unit_price,
    oi.quantity,
    o.amount,
    s.opened_date,
    INITCAP(TRIM(o.channel)) AS channel_title
FROM order_items oi
LEFT OUTER JOIN orders o ON o.id = oi.order_id
JOIN stores s ON o.store_id = s.id
ORDER BY s.opened_date;
