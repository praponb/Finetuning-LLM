SELECT carrier, COUNT(CASE WHEN carrier = 'DHL' THEN 1 END) AS dhl_count
FROM shipments
WHERE order_id IS NOT NULL AND (status IN ('LOST', 'RETURNED') OR delivered_date <= '2025-03-01')
GROUP BY carrier
ORDER BY dhl_count
