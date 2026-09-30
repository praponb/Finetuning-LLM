SELECT *, ADD_MONTHS(shipped_date, 3) AS shipped_date_plus_months
FROM shipments
WHERE delivered_date >= '2024-11-01' AND delivered_date < '2025-01-01';
