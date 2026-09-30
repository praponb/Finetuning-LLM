SELECT carrier, SUM(CASE WHEN carrier = 'DHL' THEN cost ELSE 0 END) AS dhl_cost
FROM shipments
GROUP BY carrier
ORDER BY carrier
