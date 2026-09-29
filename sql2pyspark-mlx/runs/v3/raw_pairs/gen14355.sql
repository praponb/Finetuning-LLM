SELECT
    s.order_id,
    o.amount,
    o.discount,
    o.status
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
WHERE s.carrier IN ('UPS', 'DHL', 'Kerry') AND o.channel NOT IN ('mobile', 'store')
ORDER BY o.status DESC;
