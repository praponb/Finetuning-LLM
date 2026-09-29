WITH scoped AS (
    -- Scope the warehouses rows
    SELECT
        id,
        city,
        capacity,
        opened_on,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        LOWER(REGEXP_REPLACE(TRIM(city), ' ', '_')) AS city_slug
    FROM warehouses
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM scoped
    WHERE capacity_bucket > (SELECT MIN(capacity_bucket) FROM scoped)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        capacity - LAG(capacity) OVER (ORDER BY opened_on DESC, id) AS capacity_change,
        ROUND(capacity_bucket / NULLIF(SUM(capacity_bucket) OVER w_opened_on_year, 0), 4) AS capacity_bucket_share_of_opened_on_year,
        SUM(capacity) OVER (PARTITION BY YEAR(opened_on)) AS opened_on_year_sum_capacity,
        ROUND(capacity - AVG(capacity) OVER w_opened_on_year, 2) AS capacity_vs_opened_on_year_avg,
        NTH_VALUE(capacity, 2) OVER (ORDER BY opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity
    FROM qualified
    WINDOW w_opened_on_year AS (PARTITION BY YEAR(opened_on))
)
-- Final projection
SELECT
    id,
    capacity,
    capacity_bucket,
    city_slug,
    capacity_change,
    capacity_bucket_share_of_opened_on_year,
    capacity_vs_opened_on_year_avg,
    second_capacity,
    TRUNC(opened_on, 'MM') AS opened_on_month_start
FROM sequenced
WHERE opened_on_year_sum_capacity > 60700
ORDER BY second_capacity DESC NULLS FIRST, id NULLS LAST
LIMIT 20
