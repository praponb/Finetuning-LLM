WITH ValidStores AS (
    SELECT *
    FROM stores
    WHERE sqft > 5932
),
agg AS (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, QUARTER(opened_date) AS opened_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM ValidStores
    GROUP BY DATE_TRUNC('MONTH', opened_date), QUARTER(opened_date)
)
SELECT opened_month, opened_quarter
FROM agg
ORDER BY opened_quarter DESC
