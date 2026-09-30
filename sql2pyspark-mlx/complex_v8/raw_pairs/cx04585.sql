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
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        sk.labels AS sku_labels
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.city IN ('Bangkok', 'Kuala Lumpur')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator), 0), 4) AS qty_share_of_operator,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_warehouse_id_by_qty)) / 60, 1) AS minutes_since_prev_moved_at,
        LEAD(moved_at, 2) OVER (PARTITION BY warehouse_id ORDER BY unit_price, id) AS next2_moved_at,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_city), 2) AS unit_price_vs_warehouse_city_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_city, move_type ORDER BY moved_at), 3) AS moved_at_percent_rank,
        NTILE(10) OVER w_all_by_qty AS qty_decile,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_city), 0), 4) AS unit_price_share_of_warehouse_city,
        ROW_NUMBER() OVER (PARTITION BY warehouse_city, move_type ORDER BY moved_at, id) AS seq_no
    FROM with_details_warehouse
    WINDOW w_warehouse_id_by_qty AS (PARTITION BY warehouse_id ORDER BY qty, id), w_all_by_qty AS (ORDER BY qty ASC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        warehouse_city,
        sku_labels,
        qty_share_of_operator,
        next2_moved_at,
        unit_price_vs_warehouse_city_avg,
        moved_at_percent_rank,
        qty_decile,
        seq_no,
        LEAD(minutes_since_prev_moved_at) OVER w_all_by_minutes_since_prev_mo AS next_minutes_since_prev_moved_at,
        ROW_NUMBER() OVER w_seq_no_by_qty AS seq_no_2,
        ROUND(CUME_DIST() OVER w_seq_no_by_qty_ties, 3) AS qty_cume_dist,
        ROUND(AVG(qty_decile) OVER (ORDER BY minutes_since_prev_moved_at ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_decile_avg_4
    FROM metrics
    WINDOW w_all_by_minutes_since_prev_mo AS (ORDER BY minutes_since_prev_moved_at ASC NULLS LAST, id), w_seq_no_by_qty AS (PARTITION BY seq_no ORDER BY qty DESC, id), w_seq_no_by_qty_ties AS (PARTITION BY seq_no ORDER BY qty DESC)
)
SELECT
    id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price,
    sku_labels,
    qty_share_of_operator,
    next2_moved_at,
    unit_price_vs_warehouse_city_avg,
    moved_at_percent_rank,
    qty_decile,
    next_minutes_since_prev_moved_at,
    seq_no_2,
    qty_decile_avg_4,
    MONTH(moved_at) AS moved_at_month
FROM ranked
WHERE qty >= 169
ORDER BY operator DESC
