WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        city,
        capacity,
        opened_on,
        CONCAT(city, '#', CAST(id AS STRING)) AS city_ref,
        CONCAT(wh_code, '#', CAST(id AS STRING)) AS wh_code_ref,
        ABS(capacity - 4912) AS capacity_distance
    FROM warehouses
    WHERE capacity <= 11506
      AND opened_on IS NOT NULL
),
unique_rows AS (
    -- Keep the earliest row per city
    SELECT
        id,
        city,
        capacity,
        opened_on,
        city_ref,
        wh_code_ref,
        capacity_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM unique_rows s
    WHERE s.id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.move_type = 'INBOUND'
        )
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        opened_on,
        wh_code_ref,
        SUM(capacity_distance) OVER (PARTITION BY city) AS city_sum_capacity_distance,
        MIN(capacity_distance) OVER (PARTITION BY city) AS city_min_capacity_distance,
        ROUND(capacity_distance - AVG(capacity_distance) OVER w_city, 2) AS capacity_distance_vs_city_avg,
        NTILE(4) OVER (PARTITION BY city, MONTH(opened_on) ORDER BY capacity, id) AS capacity_quartile
    FROM screened
    WINDOW w_city AS (PARTITION BY city)
)
SELECT id, measure, value
FROM (
    SELECT id, city_min_capacity_distance, city_sum_capacity_distance
    FROM with_trends
) src
UNPIVOT (
    value FOR measure IN (city_min_capacity_distance, city_sum_capacity_distance)
) unp
