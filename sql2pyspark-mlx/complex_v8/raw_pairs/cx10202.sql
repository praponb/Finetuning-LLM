WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
        LENGTH(TRIM(move_type)) AS move_type_len,
        LENGTH(TRIM(move_type)) AS move_type_len_2
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        move_type_prefix,
        move_type_len,
        move_type_len_2,
        COALESCE(move_type_len, 0) AS move_type_len_filled,
        FLOOR(move_type_len / 2) * 2 AS move_type_len_bucket
    FROM scoped
    WHERE operator IS NOT NULL
),
latest AS (
    -- Keep the latest row per operator, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        move_type_prefix,
        move_type_len,
        move_type_len_2,
        move_type_len_filled,
        move_type_len_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY moved_at DESC, id DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(operator)) AS operator_len,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM latest
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type_prefix,
        move_type_len_filled,
        operator_len,
        days_since_moved_at,
        SUM(move_type_len_2) OVER (PARTITION BY sku_id) AS sku_id_sum_move_type_len_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, move_type_len ORDER BY move_type_len ASC, id), 3) AS move_type_len_percent_rank,
        LEAD(move_type_len_2, 2) OVER (PARTITION BY operator ORDER BY operator_len NULLS LAST, id) AS next2_move_type_len_2
    FROM enriched_2
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type_prefix,
        s.move_type_len_filled,
        s.days_since_moved_at,
        s.sku_id_sum_move_type_len_2,
        s.move_type_len_percent_rank,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM metrics s
    JOIN warehouses w ON w.id = s.warehouse_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(days_since_moved_at) OVER (PARTITION BY warehouse_city ORDER BY move_type_len_filled DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_moved_at_avg_1_prior,
        MAX(move_type_len_filled) OVER (PARTITION BY warehouse_city ORDER BY move_type_len_filled DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS move_type_len_filled_max_3_prior,
        ROUND(move_type_len_percent_rank - AVG(move_type_len_percent_rank) OVER (PARTITION BY warehouse_id, move_type_len_filled), 2) AS move_type_len_percent_rank_vs_warehouse_id_move_
    FROM joined
)
SELECT
    id,
    warehouse_id,
    move_type_prefix,
    days_since_moved_at,
    move_type_len_percent_rank,
    warehouse_city,
    move_type_len_filled_max_3_prior,
    move_type_len_percent_rank_vs_warehouse_id_move_,
    CONCAT_WS(' / ', move_type_prefix, warehouse_city) AS move_type_prefix_warehouse_city_label
FROM ranked
ORDER BY warehouse_city, id DESC NULLS FIRST
LIMIT 10
