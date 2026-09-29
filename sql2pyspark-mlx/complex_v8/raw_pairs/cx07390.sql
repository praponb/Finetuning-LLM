WITH recent AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        DATEDIFF(DATE '2025-06-30', opened_on) AS days_since_opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM warehouses
    WHERE capacity < 11506
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.opened_on,
        s.days_since_opened_on,
        s.city_or_unknown,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price
    FROM recent s
    JOIN stock_moves st ON st.warehouse_id = s.id AND st.unit_price BETWEEN 73.0 AND 367.7
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(city)) AS city_len,
        COALESCE(city_or_unknown, 'UNKNOWN') AS city_or_unknown_or_unknown,
        CASE WHEN city IN ('Bangkok', 'Kuala Lumpur') THEN 'group_a' WHEN city = 'Singapore' THEN 'group_b' ELSE 'other' END AS city_group
    FROM exploded_detail
    WHERE days_since_opened_on BETWEEN 140 AND 665
),
city_or_unknown_or_unknown_stats AS (
    -- Max days_since_opened_on per city_or_unknown_or_unknown
    SELECT city_or_unknown_or_unknown, MAX(days_since_opened_on) AS max_days_since_opened_on
    FROM scored
    GROUP BY city_or_unknown_or_unknown
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_opened_on,
        ROUND(s.days_since_opened_on - g.max_days_since_opened_on, 2) AS days_since_opened_on_minus_max
    FROM scored s
    JOIN city_or_unknown_or_unknown_stats g ON g.city_or_unknown_or_unknown = s.city_or_unknown_or_unknown
    WHERE s.days_since_opened_on = g.max_days_since_opened_on
)
SELECT
    id,
    wh_code,
    city,
    opened_on,
    city_or_unknown,
    move_id,
    move_moved_at,
    move_unit_price,
    city_len,
    city_or_unknown_or_unknown,
    city_group,
    max_days_since_opened_on,
    days_since_opened_on_minus_max,
    GREATEST(days_since_opened_on, move_unit_price) AS greatest_days_since_opened_on_move_unit_price
FROM compared
WHERE city <> 'Singapore'
ORDER BY city_or_unknown_or_unknown DESC
