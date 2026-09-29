WITH base_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LENGTH(TRIM(wh_code)) AS wh_code_len,
        CASE WHEN city IN ('Kuala Lumpur', 'Bangkok') THEN 'group_a' WHEN city = 'Singapore' THEN 'group_b' ELSE 'other' END AS city_group
    FROM warehouses
    WHERE capacity > 2614
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MIN(moved_at) AS first_moved_at, MIN(qty) AS min_move_qty
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at,
        st.min_move_qty
    FROM base_warehouses s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
latest_per_key AS (
    -- Keep the latest row per city_group, city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        wh_code_len,
        city_group,
        first_moved_at,
        min_move_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city_group, city ORDER BY first_moved_at DESC, id DESC) AS recency_rank
        FROM with_counts
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with CUBE
SELECT
    COALESCE(city, 'ALL') AS city,
    COALESCE(city_group, 'ALL') AS city_group,
    wh_code_len,
    ROUND(SUM(min_move_qty) / NULLIF(SUM(capacity), 0), 3) AS min_move_qty_to_capacity_ratio,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(min_move_qty) AS min_min_move_qty
FROM latest_per_key
GROUP BY CUBE(city, city_group, wh_code_len)
ORDER BY city NULLS LAST, city_group NULLS LAST, wh_code_len NULLS LAST
