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
        unit_price
    FROM stock_moves
    WHERE unit_price > 206.8
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price NULLS LAST, id), 3) AS unit_price_percent_rank,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY operator, warehouse_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty
    FROM filtered
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price_percent_rank,
        sk.is_active AS sku_is_active
    FROM windowed s
    JOIN skus sk ON sk.id = s.sku_id AND sk.sku_code IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        move_type,
        moved_at,
        unit_price_percent_rank,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref,
        ABS(unit_price_percent_rank - 0.5) AS unit_price_percent_rank_distance,
        ROUND(unit_price_percent_rank * 0.9, 2) AS unit_price_percent_rank_adj
    FROM joined_sku
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(move_type_ref, '#', CAST(id AS STRING)) AS move_type_ref_ref,
        UPPER(TRIM(move_type)) AS move_type_clean
    FROM tagged
    WHERE move_type = 'INBOUND'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        move_type,
        unit_price_percent_rank_distance,
        unit_price_percent_rank_adj,
        move_type_clean,
        MIN(unit_price_percent_rank) OVER w_moved_at_month_move_type AS moved_at_month_move_type_min_unit_price_percent_,
        LAG(moved_at) OVER (PARTITION BY MONTH(moved_at), move_type ORDER BY moved_at NULLS LAST, id) AS prev_moved_at
    FROM tagged_2
    WINDOW w_moved_at_month_move_type AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), move_type)
)
SELECT
    id,
    unit_price_percent_rank_distance,
    unit_price_percent_rank_adj,
    move_type_clean,
    moved_at_month_move_type_min_unit_price_percent_,
    LOWER(REGEXP_REPLACE(TRIM(move_type_clean), ' ', '_')) AS move_type_clean_slug
FROM metrics
ORDER BY id DESC, move_type_clean_slug DESC
