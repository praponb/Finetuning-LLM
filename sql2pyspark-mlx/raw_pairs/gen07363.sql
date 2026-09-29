WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(store_name, ' ', city) AS store_name_city
    FROM stores
    WHERE sqft >= 6695
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        MIN(sqft) OVER w_running AS last4_min_sqft,
        COUNT(sqft) OVER w_running AS last4_count_sqft,
        MAX(sqft) OVER w_running AS last4_max_sqft
    FROM cleaned
    WINDOW w_running AS (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.revenue >= 1300
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        region,
        opened_date
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY last4_min_sqft NULLS FIRST) AS row_num
        FROM not_flagged
    ) x
    WHERE row_num = 1
),
ranked_stores AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS rn
        FROM ranked
    ) x
    WHERE rn = 1
)
-- Grouped metrics
SELECT
    region,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    COUNT(*) FILTER (WHERE region = 'North') AS north_rows,
    COUNT(CASE WHEN region = 'North' THEN 1 END) AS north_count,
    COUNT_IF(region = 'Central') AS central_events,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list_2
FROM ranked_stores
GROUP BY ROLLUP (region, DATE_TRUNC('QUARTER', opened_date))
ORDER BY region NULLS LAST;
