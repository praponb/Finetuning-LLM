SELECT
    o.amount,
    o.store_id,
    o.discount,
    s.carrier,
    s.status,
    CASE WHEN s.carrier IN ('Kerry', 'DHL') THEN 'Priority' ELSE 'Other' END AS carrier_group
FROM orders o
LEFT OUTER JOIN shipments s ON o.id = s.order_id
ORDER BY amount DESC, s.status DESC
