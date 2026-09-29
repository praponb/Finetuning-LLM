WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE move_type = 'INBOUND'
      AND operator IN ('op_ann', 'op_chan', 'op_dew')
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        sk.unit_cost AS sku_unit_cost
    FROM valid_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC) AS moved_at_rank,
        RANK() OVER w_warehouse_id_sku_id_by_moved_2 AS moved_at_rank_2,
        NTH_VALUE(moved_at, 2) OVER (ORDER BY sku_unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        LAG(moved_at_dayofweek) OVER (PARTITION BY warehouse_id ORDER BY sku_unit_cost DESC, id) AS prev_moved_at_dayofweek,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC), 3) AS moved_at_percent_rank,
        ROUND(qty - AVG(qty) OVER w_warehouse_id, 2) AS qty_vs_warehouse_id_avg,
        sku_unit_cost - LAG(sku_unit_cost) OVER (PARTITION BY warehouse_id ORDER BY sku_unit_cost DESC, id) AS sku_unit_cost_change,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC, id) AS moved_at_dense_rank,
        FIRST_VALUE(moved_at) OVER (PARTITION BY warehouse_id ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        ROUND(moved_at_dayofweek / NULLIF(SUM(moved_at_dayofweek) OVER (PARTITION BY warehouse_id), 0), 4) AS moved_at_dayofweek_share_of_warehouse_id,
        DENSE_RANK() OVER w_all_by_sku_unit_cost_ties AS sku_unit_cost_dense_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_sku_unit_cost_ties, 3) AS sku_unit_cost_percent_rank,
        SUM(moved_at_dayofweek) OVER (PARTITION BY warehouse_id ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS moved_at_dayofweek_sum_2
    FROM joined_warehouse
    WINDOW w_warehouse_id_sku_id_by_moved_2 AS (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC, id), w_warehouse_id AS (PARTITION BY warehouse_id), w_all_by_sku_unit_cost_ties AS (ORDER BY sku_unit_cost ASC)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        moved_at_dayofweek,
        warehouse_wh_code,
        warehouse_city,
        warehouse_opened_on,
        sku_unit_cost,
        moved_at_rank,
        second_moved_at,
        prev_moved_at_dayofweek,
        moved_at_percent_rank,
        qty_vs_warehouse_id_avg,
        sku_unit_cost_change,
        moved_at_dense_rank,
        first_moved_at,
        distinct_sku_id_in_warehouse_id,
        moved_at_dayofweek_share_of_warehouse_id,
        sku_unit_cost_dense_rank,
        sku_unit_cost_percent_rank,
        moved_at_dayofweek_sum_2
    FROM ranked
    WHERE moved_at_rank_2 <= 5
      AND moved_at < TIMESTAMP '2025-02-24 08:00:00'
)
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator,
    moved_at_dayofweek,
    warehouse_wh_code,
    warehouse_city,
    warehouse_opened_on,
    sku_unit_cost,
    moved_at_rank,
    second_moved_at,
    prev_moved_at_dayofweek,
    moved_at_percent_rank,
    qty_vs_warehouse_id_avg,
    sku_unit_cost_change,
    moved_at_dayofweek_share_of_warehouse_id,
    sku_unit_cost_percent_rank,
    moved_at_dayofweek_sum_2
FROM top_rows
WHERE first_moved_at < TIMESTAMP '2025-02-12 13:00:00'
