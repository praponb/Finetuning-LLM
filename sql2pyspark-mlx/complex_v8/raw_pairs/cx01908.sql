WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE move_type = 'ADJUST'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        move_type,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        LENGTH(TRIM(move_type)) AS move_type_len,
        CASE WHEN unit_price >= 330.0 THEN 'large' WHEN unit_price >= 160.8 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM filtered
    WHERE MONTH(moved_at) IN (1, 2)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type_slug,
        move_type_len,
        ROUND(move_type_len / NULLIF(SUM(move_type_len) OVER (PARTITION BY warehouse_id, unit_price_tier), 0), 4) AS move_type_len_share_of_warehouse_id_unit_price_t,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_id, unit_price_tier), 0), 4) AS unit_price_share_of_warehouse_id_unit_price_tier,
        NTILE(10) OVER w_move_type_len_unit_price_tie AS move_type_len_decile
    FROM tagged
    WINDOW w_move_type_len_unit_price_tie AS (PARTITION BY move_type_len, unit_price_tier ORDER BY move_type_len ASC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        move_type_slug,
        move_type_len,
        move_type_len_decile,
        FLOOR(move_type_len / 2) * 2 AS move_type_len_bucket,
        ABS(move_type_len - 8) AS move_type_len_distance,
        LOWER(REGEXP_REPLACE(TRIM(move_type_slug), ' ', '_')) AS move_type_slug_slug
    FROM ranked
    WHERE warehouse_id IS NOT NULL
),
warehouse_id_stats AS (
    -- Avg move_type_len_distance per warehouse_id
    SELECT warehouse_id, AVG(move_type_len_distance) AS avg_move_type_len_distance
    FROM derived
    GROUP BY warehouse_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_move_type_len_distance,
        ROUND(s.move_type_len_distance - g.avg_move_type_len_distance, 2) AS move_type_len_distance_minus_avg
    FROM derived s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.move_type_len_distance > g.avg_move_type_len_distance
)
SELECT
    id,
    warehouse_id,
    move_type_slug,
    move_type_len,
    move_type_len_decile,
    move_type_len_bucket,
    move_type_slug_slug,
    avg_move_type_len_distance,
    move_type_len_distance_minus_avg
FROM outliers
WHERE move_type_len_bucket < 10
