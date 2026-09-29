WITH matched_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM order_items
    EXCEPT ALL
    SELECT product_id, id
    FROM order_items
    WHERE unit_price <= 340
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    JOIN matched_keys k ON s.product_id = k.product_id AND s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(unit_price) OVER (PARTITION BY product_id) AS product_id_count_unit_price,
        CORR(unit_price, quantity) OVER w_grp AS corr_unit_price_quantity,
        CORR(quantity, unit_price) OVER w_hist AS corr_quantity_unit_price,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS LAST) AS unit_price_cume_dist,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY order_id), w_hist AS (PARTITION BY order_id, product_id)
),
scored_order_items AS (
    -- Ranking and gap features
    SELECT
        unit_price,
        product_id_count_unit_price,
        corr_quantity_unit_price,
        unit_price_cume_dist,
        quantity_share,
        RANK() OVER (ORDER BY quantity DESC) AS quantity_rank,
        MAX(quantity_share) OVER (PARTITION BY product_id, order_id) AS product_id_max_quantity_share
    FROM scored
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        product_id_count_unit_price,
        corr_quantity_unit_price,
        quantity_rank,
        product_id_max_quantity_share,
        CUME_DIST() OVER w_running AS unit_price_cume_dist_2,
        PERCENT_RANK() OVER w_running_2 AS quantity_share_percent_rank,
        PERCENT_RANK() OVER w_recent AS unit_price_cume_dist_percent_rank
    FROM scored_order_items
    WINDOW w_running AS (ORDER BY unit_price DESC), w_running_2 AS (ORDER BY quantity_share DESC), w_recent AS (ORDER BY unit_price_cume_dist DESC)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity_rank / NULLIF(corr_quantity_unit_price, 0) AS quantity_rank_per_corr_quantity_unit_price,
        CAST(unit_price_cume_dist_2 AS INT) AS unit_price_cume_dist_2_int
    FROM with_windows
    WHERE product_id_max_quantity_share <= 0
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY quantity_rank NULLS FIRST) AS quantity_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY unit_price_cume_dist_2_int NULLS FIRST) AS unit_price_cume_dist_2_int_cume_dist,
        PERCENT_RANK() OVER w_running AS unit_price_cume_dist_2_percent_rank
    FROM cleaned
    WINDOW w_running AS (ORDER BY unit_price_cume_dist_2)
)
-- Multi-level aggregation
SELECT
    CASE WHEN unit_price_cume_dist_2_int_cume_dist >= 0.62 THEN 'HIGH' ELSE 'LOW' END AS unit_price_cume_dist_2_int_cume_dist_tier,
    CASE WHEN quantity_rank_percent_rank >= 0.62 THEN 'LARGE' ELSE 'SMALL' END AS quantity_rank_percent_rank_tier,
    CASE WHEN product_id_max_quantity_share >= 1 THEN 'HIGH' ELSE 'LOW' END AS product_id_max_quantity_share_tier,
    APPROX_PERCENTILE(unit_price_cume_dist_2, 0.5) AS unit_price_cume_dist_2_p50,
    ROUND(AVG(quantity_share_percent_rank), 2) AS avg_quantity_share_percent_rank,
    COUNT(*) AS row_count,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(quantity_rank, ARRAY(0.25, 0.5, 0.75)) AS quantity_rank_quartiles,
    COUNT(*) AS num_rows_2
FROM metrics
GROUP BY CASE WHEN unit_price_cume_dist_2_int_cume_dist >= 0.62 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN quantity_rank_percent_rank >= 0.62 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN product_id_max_quantity_share >= 1 THEN 'HIGH' ELSE 'LOW' END
