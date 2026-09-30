WITH scoped AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket
    FROM warehouses
    WHERE opened_on >= DATE '2024-04-13'
      AND LOWER(wh_code) LIKE '%ku%'
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.move_type = 'INBOUND' THEN 1 END) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_move_type_count,
        (SELECT ROUND(AVG(st.unit_price), 2) FROM stock_moves st WHERE st.warehouse_id = s.id) AS avg_move_unit_price
    FROM scoped s
)
SELECT
    city,
    city_or_unknown,
    move_move_type_count,
    ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(wh_code)) LIKE 'K%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_wh_code_like_k
FROM with_stats
GROUP BY CUBE(city, city_or_unknown, move_move_type_count)
