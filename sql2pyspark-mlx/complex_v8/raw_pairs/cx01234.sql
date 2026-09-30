WITH scoped_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE opened_on < DATE '2025-07-03'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY city, YEAR(opened_on)) AS rows_in_city_opened_on_year,
        MIN(capacity) OVER (PARTITION BY city, YEAR(opened_on)) AS city_opened_on_year_min_capacity
    FROM scoped_warehouses
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE rows_in_city_opened_on_year < (SELECT MAX(rows_in_city_opened_on_year) FROM with_trends)
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    capacity,
    rows_in_city_opened_on_year,
    city_opened_on_year_min_capacity,
    CONCAT_WS('|', city, wh_code) AS city_wh_code_label,
    DAYOFWEEK(opened_on) AS opened_on_dayofweek
FROM screened
WHERE rows_in_city_opened_on_year >= 7
ORDER BY rows_in_city_opened_on_year
