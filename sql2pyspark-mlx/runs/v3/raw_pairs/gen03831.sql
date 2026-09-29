SELECT o.status, ROUND((100 * SUM(CASE WHEN s.carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_dhl, COUNT(CASE WHEN s.status = 'RETURNED' THEN 1 END) AS returned_count
FROM shipments s
LEFT OUTER JOIN orders o ON o.id = s.order_id
GROUP BY o.status;
