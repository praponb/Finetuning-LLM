SELECT id, status, ADD_MONTHS(shipped_date, 1) AS shipped_date_plus_months
FROM shipments
WHERE YEAR(shipped_date) >= 2024 AND order_id IS NOT NULL;
