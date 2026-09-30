WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CONCAT_WS('-', city, wh_code) AS city_wh_code_label,
        DATE_ADD(opened_on, 7) AS opened_on_plus_7d,
        YEAR(opened_on) AS opened_on_year
    FROM warehouses
    WHERE capacity > 2614
      AND UPPER(TRIM(wh_code)) LIKE 'B%'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_wh_code_label,
        opened_on_year,
        DENSE_RANK() OVER (ORDER BY opened_on_plus_7d) AS opened_on_plus_7d_dense_rank,
        ROUND(opened_on_year / NULLIF(SUM(opened_on_year) OVER (PARTITION BY opened_on_year), 0), 4) AS opened_on_year_share_of_opened_on_year,
        COUNT(*) OVER (PARTITION BY opened_on_year) AS rows_in_opened_on_year
    FROM recent
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(opened_on) OVER (ORDER BY opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_on,
        RANK() OVER (ORDER BY opened_on DESC, id) AS opened_on_rank
    FROM windowed
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_wh_code_label,
        opened_on_year,
        opened_on_plus_7d_dense_rank,
        opened_on_year_share_of_opened_on_year,
        rows_in_opened_on_year,
        first_opened_on
    FROM with_trends
    WHERE opened_on_rank <= 3
      AND rows_in_opened_on_year <= 17
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, rows_in_opened_on_year, opened_on_plus_7d_dense_rank, capacity, opened_on_year
    FROM first_rows
) src
UNPIVOT (
    value FOR metric_name IN (rows_in_opened_on_year, opened_on_plus_7d_dense_rank, capacity, opened_on_year)
) unp
