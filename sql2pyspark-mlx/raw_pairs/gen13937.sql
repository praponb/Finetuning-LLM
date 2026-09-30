WITH ValidStores AS (
    SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
    FROM stores
    WHERE region IS NOT NULL
),
agg AS (
    SELECT CASE WHEN YEAR(opened_date_month_end) < 2024 THEN 'before_2024' WHEN YEAR(opened_date_month_end) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_month_end_cohort, ROUND(AVG(sqft), 2) AS avg_sqft, ROUND((100 * SUM(CASE WHEN region = 'North' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_north
    FROM ValidStores
    GROUP BY CASE WHEN YEAR(opened_date_month_end) < 2024 THEN 'before_2024' WHEN YEAR(opened_date_month_end) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT opened_month_end_cohort, avg_sqft, pct_north
FROM agg
ORDER BY 3 DESC, opened_month_end_cohort
LIMIT 3;
