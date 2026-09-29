SELECT
    id,
    carrier,
    cost,
    DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
FROM shipments
WHERE cost BETWEEN 40 AND 74;
