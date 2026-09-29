WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty - unit_price AS qty_minus_unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        CASE WHEN unit_price >= 317.0 THEN 'large' WHEN unit_price >= 150.7 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-06 12:00:00'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix,
        HOUR(moved_at) AS moved_at_hour
    FROM scoped_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-12'
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_ym,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        ROUND(SUM(unit_price) / NULLIF(SUM(qty_minus_unit_price), 0), 3) AS unit_price_to_qty_minus_unit_price_ratio,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids
    FROM scored
    GROUP BY DATE_FORMAT(moved_at, 'yyyy-MM')
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        SIZE(move_type_list) AS move_type_list_count,
        n_rows + distinct_sku_ids AS n_rows_plus_distinct_sku_ids
    FROM rollup_base
    WHERE distinct_sku_ids < 8
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER w_distinct_warehouse_ids, 0), 4) AS n_rows_share_of_distinct_warehouse_ids,
        ROUND(AVG(move_type_list_count) OVER (ORDER BY distinct_sku_ids DESC, moved_at_ym ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS move_type_list_count_avg_6_prior,
        FIRST_VALUE(distinct_warehouse_ids) IGNORE NULLS OVER (ORDER BY distinct_sku_ids DESC, moved_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_warehouse_ids,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_warehouse_ids ORDER BY n_rows_plus_distinct_sku_ids DESC), 3) AS n_rows_plus_distinct_sku_ids_cume_dist,
        FIRST_VALUE(distinct_sku_ids) IGNORE NULLS OVER (ORDER BY distinct_sku_ids DESC, moved_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_sku_ids
    FROM annotated
    WINDOW w_distinct_warehouse_ids AS (PARTITION BY distinct_warehouse_ids)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE first_known_distinct_warehouse_ids > (SELECT AVG(first_known_distinct_warehouse_ids) FROM metrics)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        moved_at_ym,
        n_rows,
        move_type_list,
        distinct_warehouse_ids,
        n_rows_plus_distinct_sku_ids,
        n_rows_share_of_distinct_warehouse_ids,
        move_type_list_count_avg_6_prior,
        first_known_distinct_warehouse_ids,
        first_known_distinct_sku_ids,
        DENSE_RANK() OVER w_first_known_distinct_warehou AS n_rows_dense_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_move_type_list_count, 3) AS move_type_list_count_avg_6_prior_percent_rank,
        LEAD(distinct_sku_ids, 1, 0) OVER (PARTITION BY first_known_distinct_warehouse_ids, distinct_warehouse_ids ORDER BY n_rows ASC NULLS LAST, moved_at_ym) AS next_distinct_sku_ids
    FROM without_matches
    WINDOW w_first_known_distinct_warehou AS (PARTITION BY first_known_distinct_warehouse_ids, distinct_warehouse_ids ORDER BY n_rows ASC NULLS LAST), w_all_by_move_type_list_count AS (ORDER BY move_type_list_count_avg_6_prior DESC)
)
SELECT
    moved_at_ym,
    n_rows,
    move_type_list,
    distinct_warehouse_ids,
    n_rows_share_of_distinct_warehouse_ids,
    move_type_list_count_avg_6_prior,
    first_known_distinct_warehouse_ids,
    n_rows_dense_rank,
    move_type_list_count_avg_6_prior_percent_rank,
    next_distinct_sku_ids,
    ROUND(next_distinct_sku_ids / NULLIF(n_rows, 0), 3) AS next_distinct_sku_ids_per_n_rows,
    ARRAY_JOIN(ARRAY_SORT(move_type_list), ',') AS move_type_list_text
FROM ranked
ORDER BY next_distinct_sku_ids_per_n_rows DESC, move_type_list_count_avg_6_prior_percent_rank DESC NULLS FIRST
