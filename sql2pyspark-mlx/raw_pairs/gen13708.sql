SELECT o.status, COUNT(CASE WHEN s.carrier = 'DHL' THEN 1 END) AS dhl_count
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.shipping_fee > 33
GROUP BY o.status
ORDER BY dhl_count
