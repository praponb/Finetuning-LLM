WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(opened_date, 1) AS opened_date_plus_months,
        CONCAT_WS('/', store_name, city) AS store_name_city_key
    FROM stores
    WHERE opened_date > TO_DATE('2023-06-08')
      AND region IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date, id) AS row_num
    FROM base
    QUALIFY row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        region,
        opened_date_plus_months,
        store_name_city_key,
        row_num,
        SUM(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        MAX(sqft) OVER (PARTITION BY city, region) AS city_max_sqft,
        SUM(sqft) OVER (PARTITION BY YEAR(opened_date) ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_2
    FROM ranked
    WHERE LENGTH(store_name) < 5
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.opened_date_plus_months,
        s.row_num,
        s.city_max_sqft,
        latest.order_channel,
        latest.shipping_fee
    FROM metrics s
    JOIN LATERAL (
            SELECT c.channel AS order_channel, c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(city_max_sqft) OVER (PARTITION BY order_channel, region ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_city_max_sqft,
        MAX(city_max_sqft) OVER (PARTITION BY order_channel, region ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_city_max_sqft,
        SUM(city_max_sqft) OVER (PARTITION BY order_channel, region ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_city_max_sqft,
        AVG(city_max_sqft) OVER (PARTITION BY order_channel, region ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_city_max_sqft
    FROM latest_match
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics_stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date >= s.opened_date_plus_months
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', opened_date_plus_months) AS week_start,
    region,
    SIZE(COLLECT_SET(region)) AS num_regions,
    PERCENTILE_APPROX(city_max_sqft, ARRAY(0.25, 0.5, 0.75)) AS city_max_sqft_quartiles,
    ROUND(SUM(last7_sum_city_max_sqft) / NULLIF(COUNT(CASE WHEN order_channel = 'mobile' THEN 1 END), 0), 2) AS last7_sum_city_max_sqft_per_mobile,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE region = 'South') AS south_rows,
    COUNT(CASE WHEN order_channel = 'partner' THEN 1 END) AS partner_count
FROM not_flagged
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', opened_date_plus_months), region), (DATE_TRUNC('WEEK', opened_date_plus_months)))
HAVING COUNT(CASE WHEN order_channel = 'partner' THEN 1 END) >= 1
