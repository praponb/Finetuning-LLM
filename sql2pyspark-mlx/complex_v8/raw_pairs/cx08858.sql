WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND sku_id <= 9
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        unit_price,
        FIRST_VALUE(qty) OVER (PARTITION BY operator ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        COUNT(*) OVER w_warehouse_id_sku_id AS rows_in_warehouse_id_sku_id,
        MIN(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_qty,
        LEAD(qty) OVER (ORDER BY unit_price, id) AS next_qty,
        MAX(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_max_2_prior,
        LAG(qty, 3) OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price, id) AS prev3_qty,
        LAST_VALUE(unit_price) OVER (PARTITION BY operator ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price), 3) AS unit_price_percent_rank,
        RANK() OVER w_sku_id_warehouse_id_by_unit_2 AS unit_price_rank,
        NTILE(5) OVER (ORDER BY unit_price, id) AS unit_price_quintile
    FROM valid
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id), w_warehouse_id AS (PARTITION BY warehouse_id), w_sku_id_warehouse_id_by_unit_2 AS (PARTITION BY sku_id, warehouse_id ORDER BY unit_price, id)
)
-- Final projection
SELECT
    id,
    first_qty,
    rows_in_warehouse_id_sku_id,
    warehouse_id_min_qty,
    prev3_qty,
    unit_price_percent_rank,
    unit_price_rank,
    unit_price_quintile,
    ROUND(warehouse_id_min_qty / NULLIF(first_qty, 0), 3) AS warehouse_id_min_qty_per_first_qty,
    rows_in_warehouse_id + first_qty AS rows_in_warehouse_id_plus_first_qty
FROM metrics
ORDER BY prev3_qty NULLS LAST, warehouse_id_min_qty_per_first_qty
