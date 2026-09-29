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
        unit_price
    FROM stock_moves
    WHERE unit_price <= 367.6
      AND sku_id <= 4
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_unit_price,
        DENSE_RANK() OVER (PARTITION BY operator, move_type ORDER BY qty DESC NULLS FIRST, id) AS qty_dense_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_2_prior,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, MONTH(moved_at) ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        LAST_VALUE(qty) IGNORE NULLS OVER (ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        ROUND(CUME_DIST() OVER w_all_by_unit_price_ties, 3) AS unit_price_cume_dist,
        MAX(qty) OVER (PARTITION BY move_type ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_max_2_prior,
        ROUND(AVG(qty) OVER (PARTITION BY move_type ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3
    FROM scoped_moves
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_all_by_unit_price_ties AS (ORDER BY unit_price ASC)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price,
    warehouse_id_max_unit_price,
    unit_price_avg_6_prior,
    last_known_qty,
    rows_in_sku_id,
    unit_price_cume_dist,
    qty_avg_3,
    COALESCE(rows_in_sku_id, 0) AS rows_in_sku_id_filled
FROM windowed
