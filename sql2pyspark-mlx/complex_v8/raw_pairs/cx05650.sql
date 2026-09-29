WITH prepared_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        city,
        capacity,
        opened_on,
        LENGTH(TRIM(city)) AS city_len
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%x1%'
),
city_stats AS (
    -- Avg city_len per city
    SELECT city, AVG(city_len) AS avg_city_len
    FROM prepared_warehouses
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_city_len,
        ROUND(s.city_len - g.avg_city_len, 2) AS city_len_minus_avg
    FROM prepared_warehouses s
    JOIN city_stats g ON g.city = s.city
    WHERE s.city_len > g.avg_city_len
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.city,
        s.opened_on,
        s.city_len,
        s.avg_city_len,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator,
        st.move_type,
        st.sku_id AS move_sku_id
    FROM outliers s
    JOIN stock_moves st ON st.warehouse_id = s.id AND st.move_type IS NOT NULL
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM detail_rows_moves s
    JOIN skus sk ON sk.id = s.move_sku_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY move_operator ORDER BY opened_on ASC) AS opened_on_dense_rank,
        COUNT(*) OVER w_opened_on_month_move_operato AS rows_in_opened_on_month_move_operator,
        SIZE(COLLECT_SET(city) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on), move_operator)) AS distinct_city_in_opened_on_month_move_operator,
        ROW_NUMBER() OVER (PARTITION BY move_operator ORDER BY opened_on ASC, id, move_id) AS seq_no,
        MAX(avg_city_len) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on), move_operator) AS opened_on_month_move_operator_max_avg_city_len,
        MIN(move_unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on), move_operator) AS opened_on_month_move_operator_min_move_unit_pric
    FROM with_details
    WINDOW w_opened_on_month_move_operato AS (PARTITION BY DATE_TRUNC('MONTH', opened_on), move_operator)
)
-- Final projection
SELECT
    id,
    city,
    opened_on,
    city_len,
    avg_city_len,
    move_id,
    move_unit_price,
    move_operator,
    sku_category,
    sku_is_active,
    opened_on_dense_rank,
    distinct_city_in_opened_on_month_move_operator,
    seq_no,
    opened_on_month_move_operator_max_avg_city_len,
    opened_on_month_move_operator_min_move_unit_pric
FROM windowed
ORDER BY move_id DESC, city NULLS LAST
