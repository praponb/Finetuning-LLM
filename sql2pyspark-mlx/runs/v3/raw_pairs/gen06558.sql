WITH base AS (
    SELECT *, ADD_MONTHS(opened_date, 6) AS opened_date_plus_months
    FROM stores
    WHERE LENGTH(store_name) < 9
),
Agg AS (
    SELECT CASE WHEN region IN ('North', 'South') THEN 'core' ELSE 'other' END AS region_group, ROUND(AVG(sqft), 2) AS avg_sqft, COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count
    FROM base
    GROUP BY CASE WHEN region IN ('North', 'South') THEN 'core' ELSE 'other' END
)
SELECT region_group, avg_sqft, bangkok_count
FROM Agg
ORDER BY 2;
