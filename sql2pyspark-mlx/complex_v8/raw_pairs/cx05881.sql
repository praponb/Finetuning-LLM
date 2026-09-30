WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 139) AS qty_distance
    FROM stock_moves
    WHERE unit_price > 236.5
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator, sku_id), 0), 4) AS unit_price_share_of_operator_sku_id,
        ROUND(AVG(qty_distance) OVER (PARTITION BY sku_id ORDER BY moved_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS qty_distance_avg_3_prior,
        SUM(qty_distance) OVER (ORDER BY unit_price NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_distance_sum_2_prior,
        ROUND(qty_distance / NULLIF(SUM(qty_distance) OVER w_operator_moved_at_year, 0), 4) AS qty_distance_share_of_operator_moved_at_year,
        MAX(qty_distance) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_max_qty_distance,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY qty_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        COUNT(*) OVER (PARTITION BY operator, sku_id) AS rows_in_operator_sku_id,
        FIRST_VALUE(qty_distance) IGNORE NULLS OVER (ORDER BY qty_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty_distance,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at ASC, id) AS rn
    FROM filtered
    WINDOW w_operator_moved_at_year AS (PARTITION BY operator, YEAR(moved_at))
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    operator,
    unit_price,
    qty_distance,
    unit_price_share_of_operator_sku_id,
    qty_distance_avg_3_prior,
    qty_distance_sum_2_prior,
    qty_distance_share_of_operator_moved_at_year,
    operator_sku_id_max_qty_distance,
    second_moved_at,
    rows_in_operator_sku_id
FROM with_windows
ORDER BY rows_in_operator_sku_id
