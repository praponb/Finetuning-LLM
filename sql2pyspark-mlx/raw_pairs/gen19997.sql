WITH remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM store_sales
    EXCEPT DISTINCT
    SELECT product_id, store_id
    FROM store_sales
    WHERE revenue BETWEEN 1300 AND 2300
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    JOIN remaining_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        revenue,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id, product_id), 4) AS units_share,
        STDDEV_SAMP(units) OVER (PARTITION BY store_id) AS store_id_stddev_samp_units,
        AVG(units) OVER (PARTITION BY store_id, product_id) AS store_id_avg_units,
        CORR(revenue, units) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date), store_id) AS corr_revenue_units,
        COUNT(units) OVER (PARTITION BY product_id) AS product_id_count_units
    FROM verified
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY corr_revenue_units DESC NULLS FIRST) AS corr_revenue_units_dense_rank,
        CUME_DIST() OVER (ORDER BY units_share NULLS FIRST) AS units_share_cume_dist,
        PERCENT_RANK() OVER (ORDER BY corr_revenue_units DESC NULLS LAST) AS corr_revenue_units_percent_rank,
        PERCENT_RANK() OVER (ORDER BY store_id_avg_units DESC NULLS FIRST) AS store_id_avg_units_percent_rank
    FROM metrics
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY units_share_cume_dist) AS units_share_cume_dist_rank,
        RANK() OVER (ORDER BY units_share_cume_dist) AS units_share_cume_dist_rank_2,
        CUME_DIST() OVER (ORDER BY store_id_stddev_samp_units NULLS LAST) AS store_id_stddev_samp_units_cume_dist,
        CUME_DIST() OVER (ORDER BY units_share DESC) AS units_share_cume_dist_2
    FROM with_windows
)
-- Grouped metrics
SELECT
    CASE WHEN units_share_cume_dist_2 >= 0.82 THEN 'LARGE' ELSE 'SMALL' END AS units_share_cume_dist_2_tier,
    CASE WHEN corr_revenue_units_percent_rank >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END AS corr_revenue_units_percent_rank_tier,
    APPROX_PERCENTILE(units_share, ARRAY(0.25, 0.5, 0.75)) AS units_share_quartiles,
    MAX(store_id_avg_units_percent_rank) AS max_store_id_avg_units_percent_rank,
    GROUPING_ID() AS gid
FROM scored
GROUP BY GROUPING SETS ((CASE WHEN units_share_cume_dist_2 >= 0.82 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN corr_revenue_units_percent_rank >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN units_share_cume_dist_2 >= 0.82 THEN 'LARGE' ELSE 'SMALL' END))
HAVING MAX(store_id_avg_units_percent_rank) > 0
ORDER BY max_store_id_avg_units_percent_rank;
