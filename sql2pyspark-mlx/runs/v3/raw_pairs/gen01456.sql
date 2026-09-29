SELECT cost, shipped_date, DATE_ADD(delivered_date, 64) AS delivered_date_plus_64d
FROM shipments
WHERE delivered_date < '2025-10-14' AND (delivered_date >= '2023-09-16' OR order_id IS NULL)
ORDER BY shipped_date;
