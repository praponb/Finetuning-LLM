WITH recent_stores AS (
    SELECT *, CASE WHEN city = 'New York' THEN 1 ELSE 0 END AS is_new_york
    FROM stores
    WHERE NOT (MONTH(opened_date) IN (6, 8, 12) OR opened_date > '2023-11-01')
),
opened_year_summary AS (
    SELECT
        YEAR(opened_date) AS opened_year,
        QUARTER(opened_date) AS opened_quarter,
        SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) AS south_count,
        MIN(is_new_york) AS min_is_new_york
    FROM recent_stores
    GROUP BY YEAR(opened_date), QUARTER(opened_date)
)
SELECT
    opened_year,
    opened_quarter,
    south_count,
    min_is_new_york
FROM opened_year_summary
WHERE south_count > 42
ORDER BY 4
