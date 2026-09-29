SELECT
    o.id,
    o.channel,
    COUNT(s.id) AS shipment_count,
    MIN(s.shipped_date) AS min_shipped_date
FROM orders o
LEFT JOIN shipments s ON s.order_id = o.id
WHERE s.carrier = 'UPS'
GROUP BY o.id, o.channel
HAVING shipment_count > 3;
