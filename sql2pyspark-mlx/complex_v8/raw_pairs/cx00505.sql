WITH prepared_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        DATE_TRUNC('YEAR', opened_on) AS opened_on_year,
        MONTH(opened_on) AS opened_on_month,
        ABS(capacity - 4912) AS capacity_distance
    FROM warehouses
    WHERE capacity <= 11768
),
labeled AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(opened_on_year, 'yyyy-MM') AS opened_on_year_label
    FROM prepared_warehouses
    WHERE capacity_distance <= 16000
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE capacity_distance > (SELECT AVG(capacity_distance) FROM labeled)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        wh_code,
        capacity,
        opened_on_month,
        capacity_distance,
        COUNT(*) OVER w_opened_on_month AS rows_in_opened_on_month,
        ROUND(AVG(opened_on_month) OVER (PARTITION BY opened_on_month ORDER BY opened_on_month, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS opened_on_month_avg_3_prior
    FROM filtered
    WINDOW w_opened_on_month AS (PARTITION BY opened_on_month)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, capacity_distance, capacity, opened_on_month, rows_in_opened_on_month
    FROM with_trends
) src
UNPIVOT (
    amount FOR field IN (capacity_distance AS capacity_distance_v, capacity AS m_capacity, opened_on_month AS m_opened_on_month, rows_in_opened_on_month AS rows_in_opened_on_month_v)
) unp
