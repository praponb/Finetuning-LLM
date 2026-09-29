WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date < s.opened_date
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        NTILE(5) OVER w_running sqft_bucket,
        SUM(sqft) OVER w_grp cumulative_sqft,
        AVG(sqft) OVER w_running_2 sqft_avg_3_prior
    FROM kept
    WINDOW w_running AS (ORDER BY sqft, id), w_grp AS (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_bucket,
        cumulative_sqft,
        sqft_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) seq_no,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft DESC NULLS FIRST) sqft_cume_dist
    FROM with_windows
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        COUNT(*) FILTER (WHERE region = 'Central') AS central_rows,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM metrics
    GROUP BY seq_no
)
SELECT
    seq_no,
    central_rows,
    city_list
FROM daily
ORDER BY central_rows DESC
