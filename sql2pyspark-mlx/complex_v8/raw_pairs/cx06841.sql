WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        city,
        capacity,
        opened_on,
        CONCAT_WS(' / ', city, wh_code) AS city_wh_code_label
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        city,
        capacity,
        opened_on,
        city_wh_code_label,
        NTILE(5) OVER (PARTITION BY city ORDER BY capacity, id) AS capacity_quintile,
        SIZE(COLLECT_SET(city) OVER (PARTITION BY YEAR(opened_on))) AS distinct_city_in_opened_on_year,
        RANK() OVER (ORDER BY capacity) AS capacity_rank,
        MIN(capacity) OVER (PARTITION BY city ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_capacity_so_far,
        MAX(capacity) OVER (ORDER BY capacity, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS capacity_max_1_prior
    FROM filtered
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATE_ADD(opened_on, 30) AS opened_on_plus_30d,
        FLOOR(min_capacity_so_far / 50000) * 50000 AS min_capacity_so_far_bucket
    FROM with_trends
    WHERE min_capacity_so_far <= 130350
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        city,
        capacity,
        opened_on,
        city_wh_code_label,
        capacity_quintile,
        distinct_city_in_opened_on_year,
        min_capacity_so_far,
        capacity_max_1_prior,
        opened_on_plus_30d,
        min_capacity_so_far_bucket
    FROM labeled
    WHERE capacity_rank = 1
      AND opened_on_plus_30d >= DATE '2024-05-01'
),
city_stats AS (
    -- Max min_capacity_so_far per city
    SELECT city, MAX(min_capacity_so_far) AS max_min_capacity_so_far
    FROM top_rows
    GROUP BY city
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_min_capacity_so_far,
        ROUND(s.min_capacity_so_far - g.max_min_capacity_so_far, 2) AS min_capacity_so_far_minus_max
    FROM top_rows s
    JOIN city_stats g ON g.city = s.city
    WHERE s.min_capacity_so_far = g.max_min_capacity_so_far
)
-- Final projection
SELECT
    id,
    city,
    capacity,
    distinct_city_in_opened_on_year,
    min_capacity_so_far,
    capacity_max_1_prior,
    min_capacity_so_far_bucket,
    min_capacity_so_far_minus_max,
    DATE_FORMAT(opened_on_plus_30d, 'MMM yyyy') AS opened_on_plus_30d_label
FROM outliers
ORDER BY distinct_city_in_opened_on_year, id
LIMIT 20
