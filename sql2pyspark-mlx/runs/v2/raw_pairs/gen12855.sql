SELECT
    id,
    carrier,
    delivered_date,
    QUARTER(shipped_date) AS shipped_date_quarter,
    MONTH(shipped_date) AS shipped_date_month
FROM shipments
WHERE NOT (shipped_date BETWEEN '2024-04-01' AND '2025-09-01' AND cost < 64);
