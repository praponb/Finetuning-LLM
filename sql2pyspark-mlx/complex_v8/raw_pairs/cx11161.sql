WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.moved_at_date,
        sk.unit_cost AS sku_unit_cost,
        w.opened_on AS warehouse_opened_on
    FROM base s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(warehouse_opened_on) OVER (PARTITION BY warehouse_id, sku_id ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_warehouse_opened_on,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER w_move_type, 0), 4) AS sku_unit_cost_share_of_move_type,
        SUM(sku_unit_cost) OVER (PARTITION BY move_type) AS move_type_sum_sku_unit_cost,
        ROW_NUMBER() OVER w_warehouse_id_sku_id_by_sku_u_2 AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, sku_id ORDER BY sku_unit_cost DESC), 3) AS sku_unit_cost_cume_dist,
        COUNT(*) OVER w_move_type AS rows_in_move_type,
        ROUND(qty / NULLIF(SUM(qty) OVER w_move_type, 0), 4) AS qty_share_of_move_type,
        LAG(qty) OVER (ORDER BY moved_at_date ASC, id) AS prev_qty,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior
    FROM enriched
    WINDOW w_move_type AS (PARTITION BY move_type), w_warehouse_id_sku_id_by_sku_u_2 AS (PARTITION BY warehouse_id, sku_id ORDER BY sku_unit_cost DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        moved_at_date,
        sku_unit_cost,
        warehouse_opened_on,
        last_warehouse_opened_on,
        sku_unit_cost_share_of_move_type,
        move_type_sum_sku_unit_cost,
        sku_unit_cost_cume_dist,
        rows_in_move_type,
        qty_share_of_move_type,
        prev_qty,
        qty_avg_2_prior
    FROM metrics
    WHERE rn = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(last_warehouse_opened_on, LAG(last_warehouse_opened_on) OVER (ORDER BY qty_avg_2_prior, id)) AS days_since_prev_last_warehouse_opened_on,
        ROW_NUMBER() OVER (ORDER BY moved_at, id) AS moved_at_seq,
        ROUND(rows_in_move_type / NULLIF(SUM(rows_in_move_type) OVER (PARTITION BY YEAR(last_warehouse_opened_on)), 0), 4) AS rows_in_move_type_share_of_last_warehouse_opened,
        LAST_VALUE(rows_in_move_type) OVER (ORDER BY qty_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rows_in_move_type,
        NTILE(3) OVER w_all_by_qty_avg_2_prior AS qty_avg_2_prior_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty_avg_2_prior), 3) AS qty_avg_2_prior_percent_rank,
        MAX(sku_unit_cost) OVER w_last_warehouse_opened_on_yea AS last_warehouse_opened_on_year_max_sku_unit_cost
    FROM leaders
    WINDOW w_all_by_qty_avg_2_prior AS (ORDER BY qty_avg_2_prior, id), w_last_warehouse_opened_on_yea AS (PARTITION BY YEAR(last_warehouse_opened_on))
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', last_warehouse_opened_on) AS last_warehouse_opened_on_month,
        moved_at_seq,
        MIN(moved_at_date) AS first_moved_at_date,
        ROUND(100 * COUNT(CASE WHEN move_type NOT IN ('TRANSFER', 'ADJUST') THEN 1 END) / COUNT(*), 1) AS pct_move_type_not_in_transfer_ad,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(moved_at) AS last_moved_at
    FROM windowed
    WHERE last_rows_in_move_type >= 5
    GROUP BY 1, 2
    HAVING SUM(last_rows_in_move_type) > 4
)
SELECT
    last_warehouse_opened_on_month,
    first_moved_at_date,
    pct_move_type_not_in_transfer_ad,
    distinct_ids
FROM rollup_base
WHERE moved_at_seq <= 6
ORDER BY last_warehouse_opened_on_month
