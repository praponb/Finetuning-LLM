WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY MONTH(opened_date)) AS opened_date_month_avg_sqft,
        LEAD(sqft, 2) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        MAX(sqft) OVER (PARTITION BY city) AS city_max_sqft
    FROM stores
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        opened_date,
        opened_date_month_avg_sqft,
        next_sqft,
        city_max_sqft,
        CONCAT_WS('/', store_name, city) AS store_name_city_key
    FROM metrics
    WHERE opened_date > DATE '2023-06-12'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.opened_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(city_max_sqft) OVER w_recent AS last7_sum_city_max_sqft,
        AVG(city_max_sqft) OVER w_recent AS last7_avg_city_max_sqft,
        MIN(city_max_sqft) OVER w_recent AS last7_min_city_max_sqft,
        CUME_DIST() OVER (PARTITION BY city ORDER BY next_sqft NULLS FIRST) AS next_sqft_cume_dist
    FROM eligible
    WINDOW w_recent AS (PARTITION BY city, region ORDER BY opened_date_month_avg_sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.amount AS order_amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock < s.opened_date
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        region,
        opened_date_month_avg_sqft,
        city_max_sqft,
        last7_avg_city_max_sqft,
        last7_min_city_max_sqft,
        LEAD(last7_min_city_max_sqft) OVER w_running AS next_last7_min_city_max_sqft,
        LAG(last7_min_city_max_sqft) OVER w_running AS prev_last7_min_city_max_sqft,
        LEAD(next_sqft) OVER w_running AS next_next_sqft,
        ROW_NUMBER() OVER w_running AS event_seq,
        MIN(last7_min_city_max_sqft) OVER (PARTITION BY region) AS region_min_last7_min_city_max_sqft
    FROM not_flagged
    WINDOW w_running AS (PARTITION BY city ORDER BY next_sqft, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        region,
        COUNT_IF(city = 'London') AS london_events,
        COUNT(DISTINCT region) AS distinct_regions,
        MIN(opened_date_month_avg_sqft) AS min_opened_date_month_avg_sqft,
        COUNT(*) FILTER (WHERE region = 'North') AS north_rows,
        COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM with_windows
    GROUP BY region
)
SELECT
    region,
    min_opened_date_month_avg_sqft,
    singapore_count
FROM stats
