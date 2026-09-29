SELECT delivered_date, cost
FROM shipments
WHERE order_id = 21 AND YEAR(delivered_date) = 2023 AND shipped_date >= '2025-05-01'
ORDER BY delivered_date ASC;
