SELECT
    s.id,
    s.carrier,
    s.shipped_date,
    o.customer_id,
    o.store_id,
    COALESCE(o.channel, 'N/A') AS channel_filled
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
ORDER BY customer_id DESC, id
LIMIT 10
