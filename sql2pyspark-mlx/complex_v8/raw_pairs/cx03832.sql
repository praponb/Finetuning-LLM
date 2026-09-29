WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id >= 3
      AND warehouse_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        sk.is_active AS sku_is_active
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        sku_is_active,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price ASC, id), 3) AS unit_price_percent_rank,
        SUM(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        ROUND(AVG(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_is_active, operator)) AS distinct_warehouse_id_in_sku_is_active_operator,
        LEAD(unit_price, 1, 0) OVER (ORDER BY unit_price DESC, id) AS next_unit_price,
        ROW_NUMBER() OVER w_warehouse_id_by_unit_price AS unit_price_seq,
        SUM(unit_price) OVER (ORDER BY unit_price ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_sum_3_prior,
        SIZE(COLLECT_SET(operator) OVER w_warehouse_id_sku_id) AS distinct_operator_in_warehouse_id_sku_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price DESC), 3) AS unit_price_percent_rank_2,
        COUNT(*) OVER (PARTITION BY warehouse_id ORDER BY unit_price ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER w_all_by_unit_price_ties AS unit_price_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id), 2) AS warehouse_id_sku_id_avg_unit_price,
        COUNT(*) OVER w_sku_is_active_operator AS rows_in_sku_is_active_operator,
        COUNT(*) OVER w_warehouse_id_sku_id AS rows_in_warehouse_id_sku_id
    FROM with_parent
    WINDOW w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price ASC NULLS LAST, id), w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id), w_all_by_unit_price_ties AS (ORDER BY unit_price DESC), w_sku_is_active_operator AS (PARTITION BY sku_is_active, operator)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(unit_price_percent_rank_2) OVER (PARTITION BY operator ORDER BY unit_price_percent_rank_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_percent_rank_2_so_far,
        RANK() OVER (PARTITION BY operator ORDER BY unit_price_percent_rank_2, id) AS unit_price_percent_rank_2_rank,
        LEAD(unit_price_sum_3_prior, 2) OVER (PARTITION BY sku_is_active ORDER BY next_unit_price, id) AS next2_unit_price_sum_3_prior,
        FIRST_VALUE(unit_price_seq) IGNORE NULLS OVER (PARTITION BY operator ORDER BY unit_price_percent_rank_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_seq,
        ROUND(AVG(unit_price_avg_7) OVER (PARTITION BY unit_price_rank, distinct_operator_in_warehouse_id_sku_id), 2) AS unit_price_rank_distinct_operator_in_warehouse_i,
        ROUND(AVG(rows_in_warehouse_id_sku_id) OVER (PARTITION BY distinct_warehouse_id_in_sku_is_active_operator, unit_price_seq ORDER BY rows_in_warehouse_id_sku_id DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS rows_in_warehouse_id_sku_id_avg_3_prior,
        MAX(running_count) OVER (PARTITION BY unit_price_rank, distinct_operator_in_warehouse_id_sku_id) AS unit_price_rank_distinct_operator_in_warehouse_i_2
    FROM sequenced
),
summary AS (
    -- Summarise per group
    SELECT
        sku_is_active,
        warehouse_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(*) AS n_rows,
        SUM(unit_price_percent_rank_2_rank) FILTER (WHERE NOT sku_is_active) AS unit_price_percent_rank_2_rank_not_sku_is_active,
        SUM(CASE WHEN sku_is_active = TRUE THEN running_count ELSE 0 END) AS running_count_sku_is_active_true
    FROM windowed
    GROUP BY 1, 2
)
-- Final projection
SELECT
    sku_is_active,
    operator_list,
    n_rows,
    unit_price_percent_rank_2_rank_not_sku_is_active,
    CASE WHEN n_rows >= 9 THEN 'large' WHEN n_rows >= 6 THEN 'medium' ELSE 'small' END AS n_rows_tier
FROM summary
WHERE sku_is_active = FALSE
ORDER BY unit_price_percent_rank_2_rank_not_sku_is_active DESC, n_rows_tier DESC
