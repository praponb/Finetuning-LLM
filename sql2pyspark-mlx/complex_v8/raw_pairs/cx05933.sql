WITH filtered_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug
    FROM warehouses
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        wh_code_slug,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY city), 0), 4) AS capacity_share_of_city,
        COUNT(*) OVER (PARTITION BY YEAR(opened_on), city) AS rows_in_opened_on_year_city,
        LAST_VALUE(opened_on) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_on,
        SUM(capacity) OVER (PARTITION BY city ORDER BY opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_capacity,
        SUM(capacity) OVER (PARTITION BY city ORDER BY opened_on ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS capacity_sum_2,
        COUNT(*) OVER (PARTITION BY city) AS rows_in_city,
        capacity - LAG(capacity) OVER (PARTITION BY city ORDER BY opened_on ASC, id) AS capacity_change,
        SUM(capacity) OVER w_opened_on_year_city AS opened_on_year_city_sum_capacity,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_on ASC) AS opened_on_dense_rank,
        MAX(capacity) OVER w_opened_on_year_city AS opened_on_year_city_max_capacity
    FROM filtered_warehouses
    WINDOW w_opened_on_year_city AS (PARTITION BY YEAR(opened_on), city)
),
per_group AS (
    -- Summarise per group
    SELECT
        YEAR(opened_on) AS opened_on_year,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE UPPER(TRIM(wh_code)) LIKE 'B%') AS n_upper_trim_wh_code_like_b
    FROM ranked
    GROUP BY YEAR(opened_on)
    HAVING MAX(capacity_sum_2) > 6900
)
-- Final projection
SELECT opened_on_year, distinct_ids, n_upper_trim_wh_code_like_b
FROM per_group
WHERE opened_on_year <= 2025
ORDER BY n_upper_trim_wh_code_like_b
