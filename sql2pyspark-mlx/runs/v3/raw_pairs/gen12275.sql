SELECT carrier, order_id, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
FROM shipments
GROUP BY carrier, order_id
ORDER BY carrier, returned_count DESC
