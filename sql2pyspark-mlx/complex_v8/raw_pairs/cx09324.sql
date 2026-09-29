WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        unit_price,
        operator_slug,
        RANK() OVER (PARTITION BY operator, sku_id ORDER BY moved_at NULLS LAST) AS moved_at_rank,
        MAX(unit_price) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_max_6_prior,
        NTILE(3) OVER (ORDER BY qty DESC, id) AS qty_tercile,
        ROUND(unit_price - AVG(unit_price) OVER w_move_type_operator, 2) AS unit_price_vs_move_type_operator_avg,
        COUNT(*) OVER (PARTITION BY operator, sku_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        unit_price - LAG(unit_price) OVER (ORDER BY qty DESC, id) AS unit_price_change,
        SUM(qty) OVER w_move_type_operator AS move_type_operator_sum_qty,
        FIRST_VALUE(qty) OVER (PARTITION BY operator, sku_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS seq_no,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_move_type_operator, 0), 4) AS unit_price_share_of_move_type_operator,
        MIN(qty) OVER (PARTITION BY move_type, operator) AS move_type_operator_min_qty
    FROM filtered_moves
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE seq_no <= 5
      AND unit_price <= 330.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        operator_slug,
        qty_tercile,
        running_count,
        unit_price_change,
        first_qty,
        seq_no,
        move_type_operator_min_qty,
        MIN(unit_price_max_6_prior) OVER (PARTITION BY seq_no, operator) AS seq_no_operator_min_unit_price_max_6_prior,
        ROUND(moved_at_rank - AVG(moved_at_rank) OVER w_seq_no_operator, 2) AS moved_at_rank_vs_seq_no_operator_avg,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY seq_no, operator)) AS distinct_sku_id_in_seq_no_operator,
        NTH_VALUE(running_count, 2) OVER (ORDER BY unit_price_vs_move_type_operator_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_count
    FROM top_per_group
    WINDOW w_seq_no_operator AS (PARTITION BY seq_no, operator)
),
summary AS (
    -- Group-level totals
    SELECT
        seq_no,
        move_type,
        qty_tercile,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        COUNT(*) AS n_rows
    FROM ranked
    GROUP BY seq_no, move_type, qty_tercile
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        seq_no,
        move_type,
        qty_tercile,
        distinct_move_types,
        n_rows,
        MIN(distinct_move_types) OVER w_qty_tercile AS qty_tercile_min_distinct_move_types,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY qty_tercile)) AS distinct_move_type_in_qty_tercile,
        NTH_VALUE(seq_no, 2) OVER (PARTITION BY seq_no ORDER BY distinct_move_types DESC, seq_no, move_type, qty_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_seq_no,
        ROUND(qty_tercile - AVG(qty_tercile) OVER (PARTITION BY qty_tercile), 2) AS qty_tercile_vs_qty_tercile_avg
    FROM summary
    WINDOW w_qty_tercile AS (PARTITION BY qty_tercile)
)
-- Final projection
SELECT
    seq_no,
    distinct_move_types,
    n_rows,
    qty_tercile_min_distinct_move_types,
    distinct_move_type_in_qty_tercile,
    qty_tercile_vs_qty_tercile_avg,
    COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
    ROUND(second_seq_no / NULLIF(distinct_move_types, 0), 3) AS second_seq_no_per_distinct_move_types
FROM sequenced
ORDER BY n_rows DESC
