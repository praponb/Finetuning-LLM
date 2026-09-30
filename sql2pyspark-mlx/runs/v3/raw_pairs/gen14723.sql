SELECT
    MONTH(delivered_date) AS delivered_month,
    YEAR(shipped_date) AS shipped_year,
    MIN(cost) AS min_cost,
    COUNT(DISTINCT cost) AS unique_statuss,
    MAX(delivered_date) AS last_delivered_date
FROM shipments
GROUP BY MONTH(delivered_date), YEAR(shipped_date)
ORDER BY shipped_year, delivered_month
LIMIT 50;
