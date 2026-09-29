SELECT
    id,
    status,
    delivered_date,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS delivered_date_row
FROM shipments
WHERE MONTH(delivered_date) IN (8, 9, 12)
