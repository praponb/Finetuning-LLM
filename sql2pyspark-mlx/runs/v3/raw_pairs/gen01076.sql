SELECT
    id,
    cost,
    DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date,
    DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date_2
FROM shipments
ORDER BY days_since_shipped_date_2;
