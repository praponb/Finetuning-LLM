WITH valid_stores AS (
    SELECT *
    FROM stores
    WHERE store_name IS NULL OR city IN ('London', 'Chiang Mai', 'New York')
),
summary AS (
    SELECT YEAR(opened_date) AS opened_year, COUNT(*) AS cnt, SORT_ARRAY(COLLECT_SET(region)) AS region_list
    FROM valid_stores
    GROUP BY YEAR(opened_date)
)
SELECT *
FROM summary
WHERE cnt >= 7;
