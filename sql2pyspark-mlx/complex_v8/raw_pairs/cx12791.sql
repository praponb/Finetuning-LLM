WITH prepared_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug,
        ABS(capacity - 11005) AS capacity_distance,
        LENGTH(TRIM(wh_code)) AS wh_code_len
    FROM warehouses
    WHERE city = 'Singapore'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        city_slug,
        wh_code_len,
        COUNT(*) OVER (PARTITION BY city) AS rows_in_city,
        DATEDIFF(opened_on, LAG(opened_on) OVER (ORDER BY capacity, id)) AS days_since_prev_opened_on
    FROM prepared_warehouses
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE days_since_prev_opened_on < (SELECT MAX(days_since_prev_opened_on) FROM ranked)
),
unique_rows AS (
    -- Keep the earliest row per city
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        city_slug,
        wh_code_len,
        rows_in_city,
        days_since_prev_opened_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY days_since_prev_opened_on, id) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', opened_on) AS opened_on_quarter,
        SUBSTRING(TRIM(city), 1, 2) AS city_prefix
    FROM unique_rows
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.opened_on,
        s.city_slug,
        s.wh_code_len,
        s.days_since_prev_opened_on,
        s.opened_on_quarter,
        s.city_prefix,
        st.id AS move_id,
        st.move_type,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty
    FROM labeled s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM exploded_detail_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
        )
      AND UPPER(TRIM(s.wh_code)) LIKE 'B%'
)
SELECT
    id,
    city,
    city_slug,
    days_since_prev_opened_on,
    opened_on_quarter,
    city_prefix,
    move_type,
    move_sku_id,
    MONTH(opened_on) AS opened_on_month
FROM without_matches
ORDER BY city
