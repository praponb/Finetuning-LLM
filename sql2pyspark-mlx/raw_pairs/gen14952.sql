WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        SUM(quantity) OVER (PARTITION BY MONTH(last_restock), product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('month', last_restock), store_id ORDER BY quantity) AS quantity_cume_dist,
        LEAD(quantity, 2) OVER w_recent AS next_quantity
    FROM inventory
    WHERE MONTH(last_restock) IN (1, 2, 8)
    WINDOW w_recent AS (ORDER BY quantity, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(quantity_cume_dist) OVER w_hist AS store_id_max_quantity_cume_dist,
        CUME_DIST() OVER w_recent AS quantity_cume_dist_cume_dist,
        SUM(next_quantity) OVER (ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_quantity
    FROM enriched
    WINDOW w_hist AS (PARTITION BY store_id), w_recent AS (PARTITION BY product_id, store_id ORDER BY quantity_cume_dist DESC NULLS LAST)
),
scored_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        cumulative_quantity,
        store_id_max_quantity_cume_dist,
        quantity_cume_dist_cume_dist,
        COUNT(store_id_max_quantity_cume_dist) OVER (PARTITION BY store_id ORDER BY quantity_cume_dist_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_store_id_max_quantity_cume_dist,
        MIN(store_id_max_quantity_cume_dist) OVER (PARTITION BY store_id ORDER BY quantity_cume_dist_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_store_id_max_quantity_cume_dist,
        SUM(store_id_max_quantity_cume_dist) OVER (PARTITION BY store_id ORDER BY quantity_cume_dist_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_store_id_max_quantity_cume_dist
    FROM scored
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cumulative_quantity) OVER (PARTITION BY store_id ORDER BY running_count_store_id_max_quantity_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_quantity_avg_6,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id_max_quantity_cume_dist, id) AS row_seq
    FROM scored_inventory
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        d.opened_date,
        d.city,
        d.region
    FROM with_windows a
    LEFT JOIN stores d
        ON a.store_id = d.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN cumulative_quantity >= 975 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_quantity_tier,
    ROUND(SUM(store_id_max_quantity_cume_dist) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) AS store_id_max_quantity_cume_dist_per_north,
    PERCENTILE_APPROX(running_sum_store_id_max_quantity_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS running_sum_store_id_max_quantity_cume_dist_quartiles,
    MIN(running_min_store_id_max_quantity_cume_dist) AS min_running_min_store_id_max_quantity_cume_dist,
    ROUND(AVG(cumulative_quantity_avg_6), 2) AS avg_cumulative_quantity_avg_6
FROM with_dims
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', opened_date), CASE WHEN cumulative_quantity >= 975 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('QUARTER', opened_date)), ());
