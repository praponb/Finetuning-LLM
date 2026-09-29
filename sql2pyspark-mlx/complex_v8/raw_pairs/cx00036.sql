WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active,
        w.opened_on AS warehouse_opened_on
    FROM cleaned s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        MAX(sku_weight_kg) OVER (PARTITION BY sku_is_active, sku_id ORDER BY sku_weight_kg, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS sku_weight_kg_max_1_prior,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS sku_weight_kg_avg_3_prior,
        DENSE_RANK() OVER (PARTITION BY sku_is_active, sku_id ORDER BY sku_weight_kg, id) AS sku_weight_kg_dense_rank,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), DATE_TRUNC('MONTH', warehouse_opened_on)) AS rows_in_moved_at_month_warehouse_opened_on_month,
        NTILE(10) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id) AS warehouse_opened_on_decile,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id) AS seq_no,
        LAST_VALUE(sku_weight_kg) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sku_weight_kg,
        RANK() OVER w_warehouse_id_sku_is_active_b AS moved_at_rank,
        ROUND(sku_weight_kg - AVG(sku_weight_kg) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), DATE_TRUNC('MONTH', warehouse_opened_on)), 2) AS sku_weight_kg_vs_moved_at_month_warehouse_opened,
        FIRST_VALUE(warehouse_opened_on) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_warehouse_opened_on,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_details
    WINDOW w_warehouse_id_sku_is_active_b AS (PARTITION BY warehouse_id, sku_is_active ORDER BY moved_at)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(sku_weight_kg_avg_3_prior / NULLIF(SUM(sku_weight_kg_avg_3_prior) OVER (PARTITION BY sku_is_active), 0), 4) AS sku_weight_kg_avg_3_prior_share_of_sku_is_active,
        RANK() OVER (ORDER BY second_warehouse_opened_on NULLS LAST, id) AS second_warehouse_opened_on_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY second_warehouse_opened_on DESC, id) AS rn,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY sku_is_active, seq_no)) AS distinct_sku_id_in_sku_is_active_seq_no,
        ROW_NUMBER() OVER w_seq_no_second_warehouse_open AS seq_no_2,
        LEAD(first_warehouse_opened_on) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY second_warehouse_opened_on DESC, id) AS next_first_warehouse_opened_on,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY second_warehouse_opened_on DESC) AS second_warehouse_opened_on_dense_rank
    FROM sequenced
    WINDOW w_seq_no_second_warehouse_open AS (PARTITION BY seq_no, DATE_TRUNC('MONTH', second_warehouse_opened_on) ORDER BY sku_weight_kg_max_1_prior ASC NULLS LAST, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        DATE_FORMAT(warehouse_opened_on, 'yyyy-MM') AS warehouse_opened_on_ym,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        COUNT(*) AS row_count,
        ROUND(SUM(sku_weight_kg_dense_rank) / NULLIF(SUM(sku_weight_kg_avg_3_prior_share_of_sku_is_active), 0), 3) AS sku_weight_kg_dense_rank_to_sku_weight_kg_avg_3_,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(sku_weight_kg) / NULLIF(SUM(distinct_sku_id_in_sku_is_active_seq_no), 0), 3) AS sku_weight_kg_to_distinct_sku_id_in_sku_is_activ
    FROM ranked
    GROUP BY warehouse_id, DATE_FORMAT(warehouse_opened_on, 'yyyy-MM')
)
-- Final projection
SELECT
    warehouse_id,
    distinct_sku_ids,
    row_count,
    sku_weight_kg_dense_rank_to_sku_weight_kg_avg_3_,
    distinct_ids,
    sku_weight_kg_to_distinct_sku_id_in_sku_is_activ,
    INITCAP(TRIM(warehouse_opened_on_ym)) AS warehouse_opened_on_ym_clean,
    SUBSTRING(TRIM(warehouse_opened_on_ym), 1, 4) AS warehouse_opened_on_ym_prefix
FROM per_group
