WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        UPPER(region) region_upper
    FROM stores
    WHERE opened_date > DATE '2023-06-22'
      AND store_name LIKE 'Riv%'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        region_upper,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) event_seq,
        CUME_DIST() OVER w_ordered sqft_cume_dist
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY city ORDER BY sqft)
),
enriched_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        opened_date,
        region_upper,
        event_seq,
        sqft_cume_dist,
        NTILE(3) OVER (PARTITION BY region ORDER BY sqft_cume_dist, id) sqft_cume_dist_bucket,
        AVG(event_seq) OVER (PARTITION BY region, city ORDER BY sqft_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) event_seq_avg_6
    FROM enriched
    WHERE sqft_cume_dist < 0.23
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock > '2025-02-01'
        )
),
enriched_stores_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(sqft_cume_dist_bucket) OVER (PARTITION BY city ORDER BY sqft_cume_dist_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_sqft_cume_dist_bucket,
        AVG(sqft_cume_dist_bucket) OVER (PARTITION BY city ORDER BY sqft_cume_dist_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_sqft_cume_dist_bucket,
        MAX(sqft_cume_dist_bucket) OVER (PARTITION BY city ORDER BY sqft_cume_dist_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_sqft_cume_dist_bucket,
        SUM(sqft_cume_dist_bucket) OVER (PARTITION BY city ORDER BY sqft_cume_dist_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_sqft_cume_dist_bucket,
        SUM(event_seq) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_event_seq
    FROM eligible
)
-- Grouped metrics
SELECT
    CASE WHEN running_event_seq >= 39 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_event_seq_tier,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    ROUND(AVG(event_seq_avg_6), 2) AS avg_event_seq_avg_6
FROM enriched_stores_2
GROUP BY 1
HAVING avg_event_seq_avg_6 > 0
ORDER BY avg_event_seq_avg_6 NULLS FIRST
