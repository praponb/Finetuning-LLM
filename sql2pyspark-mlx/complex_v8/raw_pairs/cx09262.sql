WITH recent_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DAYOFWEEK(opened_on) AS opened_on_dayofweek,
        DATE_TRUNC('QUARTER', opened_on) AS opened_on_quarter
    FROM warehouses
    WHERE capacity BETWEEN 4912 AND 11506
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(city) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on_quarter))) AS distinct_city_in_opened_on_quarter_month,
        NTH_VALUE(opened_on, 2) OVER (ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_on,
        MIN(capacity) OVER (ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_capacity_so_far
    FROM recent_warehouses
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.move_type <> 'ADJUST'
              AND st.unit_price > 180.0
        )
      AND s.opened_on_quarter < TIMESTAMP '2025-02-17 09:00:00'
)
SELECT
    id,
    wh_code,
    city,
    opened_on,
    opened_on_dayofweek,
    opened_on_quarter,
    distinct_city_in_opened_on_quarter_month,
    second_opened_on,
    QUARTER(second_opened_on) AS second_opened_on_quarter
FROM with_activity
WHERE capacity >= 2614
ORDER BY distinct_city_in_opened_on_quarter_month, id
LIMIT 5
