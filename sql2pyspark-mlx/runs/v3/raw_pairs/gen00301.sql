SELECT carrier, MIN(delivered_date) AS min_delivered_date
FROM shipments
WHERE carrier IS NOT NULL AND shipped_date BETWEEN '2024-03-01' AND '2024-06-01'
GROUP BY carrier
