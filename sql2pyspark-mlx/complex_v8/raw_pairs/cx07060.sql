WITH prepared AS (
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
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE unit_price >= 99.1
      AND sku_id >= 15
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        qty,
        operator,
        unit_price,
        unit_price_plus_qty,
        COUNT(*) OVER w_move_type_operator AS rows_in_move_type_operator,
        MAX(qty) OVER (PARTITION BY move_type, operator) AS move_type_operator_max_qty,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY operator ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        FIRST_VALUE(unit_price_plus_qty) IGNORE NULLS OVER (PARTITION BY operator ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_plus_qty,
        SUM(unit_price_plus_qty) OVER (PARTITION BY operator ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_plus_qty_sum_2_prior
    FROM prepared
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        sku_id,
        YEAR(second_moved_at) AS second_moved_at_year,
        operator,
        ROUND(AVG(qty), 2) AS avg_qty,
        ROUND(SUM(qty) / NULLIF(SUM(unit_price), 0), 3) AS qty_to_unit_price_ratio
    FROM with_windows
    GROUP BY 1, 2, 3
)
SELECT
    sku_id,
    second_moved_at_year,
    avg_qty,
    LENGTH(TRIM(operator)) AS operator_len
FROM rollup_base
WHERE sku_id IS NOT NULL
ORDER BY operator_len DESC, sku_id DESC NULLS FIRST
