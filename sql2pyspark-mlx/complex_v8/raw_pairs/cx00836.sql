WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND qty > 145
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id) AS rn,
        NTILE(5) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at, id) AS moved_at_quintile,
        FIRST_VALUE(moved_at) OVER (PARTITION BY operator, sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_move_type, 0), 4) AS unit_price_share_of_sku_id_move_type
    FROM filtered_moves
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type)
),
grouped AS (
    -- Summarise per group
    SELECT
        moved_at_quintile,
        warehouse_id,
        rn,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE move_type IS NOT NULL) AS n_move_type_is_not_null
    FROM metrics
    WHERE MONTH(first_moved_at) IN (1, 3)
    GROUP BY moved_at_quintile, warehouse_id, rn
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, w.capacity AS warehouse_capacity
    FROM grouped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity > 11005
),
enriched_2 AS (
    -- Attach warehouse attributes
    SELECT
        s.moved_at_quintile,
        s.warehouse_id,
        s.rn,
        s.cnt,
        s.n_move_type_is_not_null,
        s.warehouse_capacity,
        w.capacity AS warehouse_capacity_2
    FROM enriched s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    moved_at_quintile,
    warehouse_id,
    rn,
    cnt,
    warehouse_capacity,
    warehouse_capacity_2,
    moved_at_quintile - cnt AS moved_at_quintile_minus_cnt,
    ABS(rn - 5) AS rn_distance
FROM enriched_2
WHERE warehouse_capacity_2 >= 4912
ORDER BY warehouse_id, warehouse_capacity_2 DESC
