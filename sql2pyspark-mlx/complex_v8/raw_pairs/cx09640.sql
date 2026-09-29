WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND qty BETWEEN 137 AND 426
),
combined_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.sku_code,
        sk.is_active AS sku_is_active
    FROM filtered s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(qty) OVER w_sku_id AS sku_id_sum_qty,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY operator, sku_is_active ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        ROW_NUMBER() OVER (ORDER BY moved_at ASC, id) AS moved_at_seq,
        ROUND(unit_price - AVG(unit_price) OVER w_sku_id, 2) AS unit_price_vs_sku_id_avg,
        MAX(qty) OVER (PARTITION BY move_type) AS move_type_max_qty,
        COUNT(*) OVER w_move_type AS rows_in_move_type,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price
    FROM combined_details
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_move_type AS (PARTITION BY move_type)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE moved_at_seq = 1
),
summary AS (
    -- Summarise per group
    SELECT
        moved_at_seq,
        MIN(moved_at_seq) AS min_moved_at_seq,
        SUM(unit_price_vs_sku_id_avg) AS total_unit_price_vs_sku_id_avg,
        COUNT(*) FILTER (WHERE operator IS NOT NULL) AS n_operator_is_not_null
    FROM leaders
    GROUP BY moved_at_seq
    HAVING MAX(unit_price) > 189.4
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        moved_at_seq,
        min_moved_at_seq,
        total_unit_price_vs_sku_id_avg,
        n_operator_is_not_null,
        LAG(moved_at_seq) OVER (ORDER BY total_unit_price_vs_sku_id_avg ASC, moved_at_seq) AS prev_moved_at_seq,
        SUM(moved_at_seq) OVER (PARTITION BY min_moved_at_seq) AS min_moved_at_seq_sum_moved_at_seq
    FROM summary
)
-- Final projection
SELECT
    moved_at_seq,
    min_moved_at_seq,
    total_unit_price_vs_sku_id_avg,
    n_operator_is_not_null,
    prev_moved_at_seq,
    min_moved_at_seq_sum_moved_at_seq
FROM with_windows
WHERE min_moved_at_seq_sum_moved_at_seq > 41
ORDER BY moved_at_seq DESC
LIMIT 10
