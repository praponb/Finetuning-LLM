WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE NOT (opened_date > '2023-11-01' OR YEAR(opened_date) = 2025)
),
Agg AS (
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS opened_month,
        region,
        ROUND(AVG(sqft), 2) AS avg_sqft,
        MIN(opened_date) AS first_opened_date
    FROM filtered_stores
    GROUP BY DATE_TRUNC('MONTH', opened_date), region
)
SELECT
    opened_month,
    region,
    avg_sqft,
    first_opened_date
FROM Agg
WHERE avg_sqft < 66;
