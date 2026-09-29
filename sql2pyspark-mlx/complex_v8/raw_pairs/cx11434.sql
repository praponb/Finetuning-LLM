WITH filtered AS (
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
        CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE operator IN ('op_ann', 'op_eve', 'op_dew')
      AND MONTH(moved_at) IN (1, 2)
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.move_type_group,
        sk.category AS sku_category,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM filtered s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator, warehouse_id), 2) AS unit_price_vs_operator_warehouse_id_avg,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator, warehouse_id), 2) AS qty_vs_operator_warehouse_id_avg,
        MAX(unit_price) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_max_unit_price,
        NTILE(5) OVER (PARTITION BY sku_category ORDER BY unit_price DESC, id) AS unit_price_quintile,
        DENSE_RANK() OVER (PARTITION BY MONTH(warehouse_opened_on), operator ORDER BY warehouse_opened_on NULLS LAST) AS warehouse_opened_on_dense_rank,
        DENSE_RANK() OVER (PARTITION BY sku_category ORDER BY unit_price DESC, id) AS unit_price_dense_rank,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY sku_category ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        SUM(qty) OVER (PARTITION BY warehouse_id, move_type_group) AS warehouse_id_move_type_group_sum_qty,
        NTILE(3) OVER (PARTITION BY sku_id ORDER BY unit_price DESC, id) AS unit_price_tercile,
        SIZE(COLLECT_SET(sku_category) OVER w_operator_warehouse_id) AS distinct_sku_category_in_operator_warehouse_id,
        SUM(unit_price) OVER w_warehouse_id_move_type_group AS warehouse_id_move_type_group_sum_unit_price,
        DENSE_RANK() OVER (PARTITION BY MONTH(warehouse_opened_on), operator ORDER BY warehouse_opened_on NULLS LAST, id) AS warehouse_opened_on_dense_rank_2,
        COUNT(*) OVER (PARTITION BY warehouse_id, move_type_group) AS rows_in_warehouse_id_move_type_group,
        ROUND(qty / NULLIF(SUM(qty) OVER w_operator_warehouse_id, 0), 4) AS qty_share_of_operator_warehouse_id,
        LAST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY sku_category ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_opened_on,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_operator_warehouse_id, 0), 4) AS unit_price_share_of_operator_warehouse_id
    FROM joined_sku
    WINDOW w_operator_warehouse_id AS (PARTITION BY operator, warehouse_id), w_warehouse_id_move_type_group AS (PARTITION BY warehouse_id, move_type_group)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_type_group,
        sku_category,
        warehouse_opened_on,
        warehouse_wh_code,
        unit_price_vs_operator_warehouse_id_avg,
        qty_vs_operator_warehouse_id_avg,
        operator_warehouse_id_max_unit_price,
        unit_price_quintile,
        warehouse_opened_on_dense_rank,
        second_warehouse_opened_on,
        warehouse_id_move_type_group_sum_qty,
        unit_price_tercile,
        distinct_sku_category_in_operator_warehouse_id,
        warehouse_id_move_type_group_sum_unit_price,
        warehouse_opened_on_dense_rank_2,
        rows_in_warehouse_id_move_type_group,
        qty_share_of_operator_warehouse_id,
        last_known_warehouse_opened_on,
        unit_price_share_of_operator_warehouse_id
    FROM metrics
    WHERE unit_price_dense_rank <= 5
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_opened_on_dense_rank_2 ORDER BY unit_price_share_of_operator_warehouse_id DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_2,
        FIRST_VALUE(unit_price_tercile) IGNORE NULLS OVER (PARTITION BY warehouse_opened_on_dense_rank_2 ORDER BY unit_price_share_of_operator_warehouse_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_tercile,
        ROUND(AVG(unit_price_quintile) OVER (PARTITION BY unit_price_tercile), 2) AS unit_price_tercile_avg_unit_price_quintile
    FROM leaders
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    operator,
    unit_price,
    move_type_group,
    warehouse_opened_on,
    qty_vs_operator_warehouse_id_avg,
    operator_warehouse_id_max_unit_price,
    unit_price_quintile,
    warehouse_opened_on_dense_rank,
    second_warehouse_opened_on,
    warehouse_id_move_type_group_sum_qty,
    unit_price_tercile,
    warehouse_id_move_type_group_sum_unit_price,
    warehouse_opened_on_dense_rank_2,
    rows_in_warehouse_id_move_type_group,
    qty_share_of_operator_warehouse_id,
    last_known_warehouse_opened_on,
    unit_price_avg_2,
    first_known_unit_price_tercile,
    SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
FROM with_trends
WHERE first_known_unit_price_tercile >= 3
ORDER BY id, rows_in_warehouse_id_move_type_group
