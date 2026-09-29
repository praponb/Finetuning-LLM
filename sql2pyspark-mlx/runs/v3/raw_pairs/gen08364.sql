SELECT
    o.id,
    o.status,
    COUNT(s.id) AS shipment_count,
    SUM(CASE WHEN s.carrier = 'FedEx' THEN 1 ELSE 0 END) AS fedex_count,
    ROUND(STDDEV(s.cost), 2) AS stddev_cost
FROM orders o
LEFT JOIN shipments s ON o.id = s.order_id
GROUP BY o.id, o.status;
