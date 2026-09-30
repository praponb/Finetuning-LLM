SELECT CASE WHEN o.shipping_fee >= 33 THEN 'high' WHEN o.shipping_fee >= 9 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, COUNT(CASE WHEN s.carrier = 'DHL' THEN 1 END) AS dhl_count, SUM(CASE WHEN s.status = 'IN_TRANSIT' THEN s.cost ELSE 0 END) AS in_transit_cost
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.order_date > '2025-08-01'
GROUP BY CASE WHEN o.shipping_fee >= 33 THEN 'high' WHEN o.shipping_fee >= 9 THEN 'mid' ELSE 'low' END
ORDER BY shipping_fee_bucket
