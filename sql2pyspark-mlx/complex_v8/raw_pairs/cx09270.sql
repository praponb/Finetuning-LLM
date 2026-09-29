WITH recent AS (
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
        CASE WHEN operator IN ('op_dew', 'op_ann') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'TRANSFER', 'OUTBOUND')
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_qty
    FROM recent s
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_group,
        s.unit_price_bucket,
        s.operator_len,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM summarized_rows s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator_group,
        unit_price_bucket,
        warehouse_capacity,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY operator_len, MONTH(moved_at)), 2) AS operator_len_moved_at_month_avg_warehouse_capaci,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator, warehouse_city ORDER BY operator_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        ROUND(CUME_DIST() OVER w_operator_warehouse_city_by_o_2, 3) AS operator_len_cume_dist,
        LAST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_opened_on,
        SUM(operator_len) OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS operator_len_sum_2_prior
    FROM joined
    WINDOW w_operator_warehouse_city_by_o_2 AS (PARTITION BY operator, warehouse_city ORDER BY operator_len)
),
latest AS (
    -- Keep the latest row per sku_id, operator_group
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator_group,
        unit_price_bucket,
        warehouse_capacity,
        operator_len_moved_at_month_avg_warehouse_capaci,
        first_known_moved_at,
        operator_len_cume_dist,
        last_known_warehouse_opened_on,
        operator_len_sum_2_prior
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, operator_group ORDER BY unit_price_bucket DESC, id DESC) AS rn
        FROM with_trends
    ) ranked
    WHERE rn = 1
),
combined_details_warehouse AS (
    -- Enrich with dimension data
    SELECT s.*, w.opened_on AS warehouse_opened_on
    FROM latest s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.opened_on IS NOT NULL
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator_group,
        s.warehouse_capacity,
        s.operator_len_sum_2_prior,
        s.warehouse_opened_on,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.move_type,
        st.qty AS move_qty,
        st.operator AS move_operator
    FROM combined_details_warehouse s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    warehouse_capacity,
    operator_len_sum_2_prior,
    warehouse_opened_on,
    move_id,
    move_moved_at,
    move_type,
    move_qty,
    move_operator,
    DATEDIFF(DATE '2026-01-01', move_moved_at) AS days_since_move_moved_at,
    FLOOR(operator_len_sum_2_prior / 5) * 5 AS operator_len_sum_2_prior_bucket
FROM exploded_detail_moves
ORDER BY move_id DESC, id
LIMIT 5
