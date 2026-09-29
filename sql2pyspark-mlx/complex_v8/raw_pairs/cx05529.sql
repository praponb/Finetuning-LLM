WITH base AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        INITCAP(TRIM(wh_code)) AS wh_code_clean
    FROM warehouses
    WHERE capacity >= 2614
      AND city NOT IN ('Kuala Lumpur', 'Bangkok')
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.capacity,
        s.wh_code_clean,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at
    FROM base s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('DAY', move_moved_at) AS move_moved_at_day,
        LENGTH(TRIM(wh_code_clean)) AS wh_code_clean_len
    FROM line_level
),
move_operator_stats AS (
    -- Avg move_unit_price per move_operator
    SELECT move_operator, AVG(move_unit_price) AS avg_move_unit_price
    FROM derived
    GROUP BY move_operator
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_move_unit_price,
        ROUND(s.move_unit_price - g.avg_move_unit_price, 2) AS move_unit_price_minus_avg
    FROM derived s
    JOIN move_operator_stats g ON g.move_operator = s.move_operator
    WHERE s.move_unit_price > g.avg_move_unit_price
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(avg_move_unit_price / 100) * 100 AS avg_move_unit_price_bucket,
        GREATEST(move_unit_price, wh_code_clean_len) AS greatest_move_unit_price_wh_code_clean_len,
        capacity - wh_code_clean_len AS capacity_minus_wh_code_clean_len
    FROM compared
),
grouped AS (
    -- Group-level totals
    SELECT
        move_operator,
        DATE_TRUNC('MONTH', move_moved_at_day) AS move_moved_at_day_month,
        COUNT(DISTINCT city) AS distinct_citys,
        ROUND(SUM(capacity) / NULLIF(SUM(greatest_move_unit_price_wh_code_clean_len), 0), 3) AS capacity_to_greatest_move_unit_price_wh_code_cle,
        SUM(CASE WHEN city IN ('Kuala Lumpur', 'Singapore') THEN greatest_move_unit_price_wh_code_clean_len ELSE 0 END) AS greatest_move_unit_price_wh_code_clean_len_city_,
        COUNT(DISTINCT move_operator) AS distinct_move_operators,
        ARRAY_SORT(COLLECT_SET(wh_code)) AS wh_code_list
    FROM tagged
    GROUP BY 1, 2
)
SELECT
    move_operator,
    distinct_citys,
    greatest_move_unit_price_wh_code_clean_len_city_,
    distinct_move_operators,
    CONCAT(move_operator, '#', CAST(distinct_citys AS STRING)) AS move_operator_ref
FROM grouped
ORDER BY move_operator
