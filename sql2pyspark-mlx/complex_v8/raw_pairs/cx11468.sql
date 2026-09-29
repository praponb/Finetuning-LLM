WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        UPPER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE warehouse_id <= 3
      AND moved_at < TIMESTAMP '2025-02-12 13:00:00'
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared_moves
    WHERE unit_price < (SELECT MAX(unit_price) FROM prepared_moves)
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.move_type IN ('TRANSFER', 'ADJUST', 'INBOUND')
              AND st.warehouse_id >= 2
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY operator ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_price_sum_1_prior,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY operator ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        MAX(unit_price) OVER w_move_type AS move_type_max_unit_price
    FROM without_matches
    WINDOW w_move_type AS (PARTITION BY move_type)
)
-- Subtotals with CUBE
SELECT
    warehouse_id,
    operator,
    sku_id,
    COUNT(*) AS cnt,
    ROUND(STDDEV(unit_price_sum_1_prior), 2) AS stddev_unit_price_sum_1_prior
FROM with_trends
GROUP BY CUBE(warehouse_id, operator, sku_id)
ORDER BY warehouse_id NULLS LAST, operator NULLS LAST, sku_id NULLS LAST
