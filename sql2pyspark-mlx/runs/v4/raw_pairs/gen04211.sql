SELECT *, DATE_FORMAT(delivered_date, 'yyyy-MM-dd') AS delivered_date_label
FROM shipments
WHERE delivered_date BETWEEN '2024-12-01' AND '2025-07-22' AND shipped_date > '2023-04-01'
ORDER BY delivered_date DESC, id
LIMIT 5
