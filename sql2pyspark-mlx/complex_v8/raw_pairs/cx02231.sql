WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE qty >= 78
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(days_since_moved_at) OVER w_move_type_sku_id AS move_type_sku_id_sum_days_since_moved_at,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_type, sku_id)) AS distinct_warehouse_id_in_move_type_sku_id,
        RANK() OVER w_moved_at_month_by_days_since AS days_since_moved_at_rank,
        COUNT(*) OVER (PARTITION BY move_type, sku_id) AS rows_in_move_type_sku_id,
        SUM(days_since_moved_at) OVER (ORDER BY days_since_moved_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS days_since_moved_at_sum_3,
        ROUND(days_since_moved_at - AVG(days_since_moved_at) OVER (PARTITION BY move_type, sku_id), 2) AS days_since_moved_at_vs_move_type_sku_id_avg,
        LEAD(unit_price, 2) OVER (ORDER BY days_since_moved_at DESC NULLS FIRST, id) AS next2_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER w_move_type_sku_id, 2) AS unit_price_vs_move_type_sku_id_avg,
        ROW_NUMBER() OVER (ORDER BY moved_at, id) AS rn
    FROM base_moves
    WINDOW w_move_type_sku_id AS (PARTITION BY move_type, sku_id), w_moved_at_month_by_days_since AS (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY days_since_moved_at)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        unit_price,
        move_type_sku_id_sum_days_since_moved_at,
        rows_in_move_type_sku_id,
        days_since_moved_at_sum_3,
        days_since_moved_at_vs_move_type_sku_id_avg,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM with_trends
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        days_since_moved_at_sum_3 - LAG(days_since_moved_at_sum_3) OVER (PARTITION BY move_type_or_unknown, warehouse_id ORDER BY move_type_sku_id_sum_days_since_moved_at, id) AS days_since_moved_at_sum_3_change,
        MIN(rows_in_move_type_sku_id) OVER w_move_type_move_type_or_unkno AS move_type_move_type_or_unknown_min_rows_in_move_,
        NTILE(4) OVER (PARTITION BY sku_id ORDER BY days_since_moved_at_vs_move_type_sku_id_avg DESC, id) AS days_since_moved_at_vs_move_type_sku_id_avg_quar,
        ROUND(rows_in_move_type_sku_id - AVG(rows_in_move_type_sku_id) OVER (PARTITION BY move_type), 2) AS rows_in_move_type_sku_id_vs_move_type_avg,
        LAST_VALUE(days_since_moved_at_sum_3) IGNORE NULLS OVER (PARTITION BY warehouse_id, sku_id ORDER BY days_since_moved_at_sum_3 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_moved_at_sum_3,
        NTH_VALUE(rows_in_move_type_sku_id, 2) OVER (PARTITION BY sku_id ORDER BY days_since_moved_at_vs_move_type_sku_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_move_type_sku_id,
        MAX(unit_price) OVER (PARTITION BY move_type, move_type_or_unknown) AS move_type_move_type_or_unknown_max_unit_price
    FROM derived
    WINDOW w_move_type_move_type_or_unkno AS (PARTITION BY move_type, move_type_or_unknown)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        days_since_moved_at_vs_move_type_sku_id_avg_quar,
        sku_id,
        warehouse_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        ROUND(100 * COUNT(CASE WHEN move_type IN ('INBOUND', 'OUTBOUND', 'TRANSFER') THEN 1 END) / COUNT(*), 1) AS pct_move_type_in_inbound_outboun,
        SUM(days_since_moved_at_vs_move_type_sku_id_avg_quar) AS total_days_since_moved_at_vs_move_type_sku_id_av
    FROM ranked
    GROUP BY 1, 2, 3
)
SELECT
    days_since_moved_at_vs_move_type_sku_id_avg_quar,
    sku_id,
    cnt,
    total_days_since_moved_at_vs_move_type_sku_id_av,
    CASE WHEN distinct_sku_ids >= 6 THEN 'HIGH' WHEN distinct_sku_ids >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_sku_ids_tier
FROM stats
WHERE pct_move_type_in_inbound_outboun > 10.0
ORDER BY total_days_since_moved_at_vs_move_type_sku_id_av NULLS LAST
