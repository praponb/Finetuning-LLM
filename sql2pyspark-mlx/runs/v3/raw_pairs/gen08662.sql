WITH src AS (
    SELECT *
    FROM shipments
    WHERE YEAR(delivered_date) >= 2025 AND shipped_date >= '2024-10-01' AND status = 'RETURNED'
),
summary AS (
    SELECT
        YEAR(delivered_date) AS delivered_year,
        MONTH(delivered_date) AS delivered_month,
        ROUND(AVG(cost), 2) AS avg_cost,
        SUM(cost) AS total_cost
    FROM src
    GROUP BY YEAR(delivered_date), MONTH(delivered_date)
)
SELECT
    delivered_year,
    delivered_month,
    avg_cost,
    total_cost
FROM summary
ORDER BY delivered_year DESC;
