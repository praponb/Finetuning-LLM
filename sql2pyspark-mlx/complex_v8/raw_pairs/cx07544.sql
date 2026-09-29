WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price <= 367.6
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_unit_price,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY warehouse_id), 0), 4) AS qty_share_of_warehouse_id,
        ROUND(AVG(qty) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_avg_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price DESC), 3) AS unit_price_percent_rank,
        SIZE(COLLECT_SET(sku_id) OVER w_warehouse_id) AS distinct_sku_id_in_warehouse_id,
        ROUND(AVG(qty) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4,
        NTH_VALUE(qty, 2) OVER (ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        LAST_VALUE(qty) IGNORE NULLS OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_id), 0), 4) AS unit_price_share_of_warehouse_id,
        ROW_NUMBER() OVER w_all_by_unit_price AS seq_no
    FROM scoped
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty_avg_2,
        unit_price_percent_rank,
        distinct_sku_id_in_warehouse_id,
        qty_avg_4,
        last_known_qty,
        seq_no,
        unit_price + qty_avg_4 AS unit_price_plus_qty_avg_4,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        ABS(seq_no - 4) AS seq_no_distance
    FROM metrics
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        qty_avg_2,
        distinct_sku_id_in_warehouse_id,
        qty_avg_4,
        last_known_qty,
        seq_no,
        unit_price_plus_qty_avg_4,
        seq_no_distance,
        ROUND(seq_no_distance - AVG(seq_no_distance) OVER (PARTITION BY warehouse_id, distinct_sku_id_in_warehouse_id), 2) AS seq_no_distance_vs_warehouse_id_distinct_sku_id_,
        ROW_NUMBER() OVER (ORDER BY seq_no NULLS LAST, id) AS seq_no_2,
        MIN(unit_price_percent_rank) OVER (PARTITION BY warehouse_id, distinct_sku_id_in_warehouse_id) AS warehouse_id_distinct_sku_id_in_warehouse_id_min
    FROM annotated
),
per_group AS (
    -- Summarise per group
    SELECT
        seq_no_distance,
        ROUND(STDDEV(seq_no), 2) AS stddev_seq_no,
        ROUND(SUM(warehouse_id_distinct_sku_id_in_warehouse_id_min) / NULLIF(SUM(seq_no_2), 0), 3) AS warehouse_id_distinct_sku_id_in_warehouse_id_min_2,
        ROUND(STDDEV(distinct_sku_id_in_warehouse_id), 2) AS stddev_distinct_sku_id_in_warehouse_id
    FROM windowed
    GROUP BY seq_no_distance
    HAVING SUM(qty_avg_4) > 175.7
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        seq_no_distance,
        COUNT(*) OVER (PARTITION BY seq_no_distance) AS rows_in_seq_no_distance,
        MIN(stddev_seq_no) OVER (PARTITION BY seq_no_distance) AS seq_no_distance_min_stddev_seq_no
    FROM per_group
)
-- Final projection
SELECT
    seq_no_distance,
    seq_no_distance_min_stddev_seq_no,
    ROUND(rows_in_seq_no_distance / NULLIF(seq_no_distance, 0), 3) AS rows_in_seq_no_distance_per_seq_no_distance,
    seq_no_distance - rows_in_seq_no_distance AS seq_no_distance_minus_rows_in_seq_no_distance
FROM metrics_2
ORDER BY seq_no_distance_minus_rows_in_seq_no_distance
