SELECT
    status,
    shipped_date,
    carrier,
    DATEDIFF(CURRENT_DATE(), shipped_date) / 365.25 AS years_since_shipped_date
FROM shipments
WHERE YEAR(delivered_date) = 2024;
