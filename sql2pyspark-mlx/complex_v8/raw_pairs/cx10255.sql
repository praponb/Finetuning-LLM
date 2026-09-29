WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket,
        CONCAT(city, '#', CAST(id AS STRING)) AS city_ref
    FROM warehouses
    WHERE capacity BETWEEN 2614 AND 11506
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_city_opened_on_month_by_open AS opened_on_dense_rank,
        MAX(capacity) OVER (PARTITION BY city ORDER BY opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_capacity_so_far,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY YEAR(opened_on), city), 0), 4) AS capacity_share_of_opened_on_year_city,
        LAG(opened_on) OVER w_city_opened_on_month_by_open_2 AS prev_opened_on
    FROM recent
    WINDOW w_city_opened_on_month_by_open AS (PARTITION BY city, MONTH(opened_on) ORDER BY opened_on ASC), w_city_opened_on_month_by_open_2 AS (PARTITION BY city, MONTH(opened_on) ORDER BY opened_on ASC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        YEAR(opened_on) AS opened_on_year,
        city,
        MIN(capacity_share_of_opened_on_year_city) AS min_capacity_share_of_opened_on_year_city,
        MAX(capacity_share_of_opened_on_year_city) AS max_capacity_share_of_opened_on_year_city,
        MAX(capacity) AS max_capacity,
        ROUND(AVG(max_capacity_so_far), 2) AS avg_max_capacity_so_far
    FROM ranked
    GROUP BY 1, 2
    HAVING SUM(capacity_bucket) > 5750
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(city, '#', CAST(opened_on_year AS STRING)) AS city_ref,
        FLOOR(max_capacity / 5000) * 5000 AS max_capacity_bucket
    FROM rollup_base
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE min_capacity_share_of_opened_on_year_city > (SELECT AVG(min_capacity_share_of_opened_on_year_city) FROM derived)
)
SELECT
    opened_on_year,
    city,
    min_capacity_share_of_opened_on_year_city,
    max_capacity_share_of_opened_on_year_city,
    avg_max_capacity_so_far,
    city_ref,
    CONCAT_WS(': ', city_ref, city) AS city_ref_city_label
FROM qualified
ORDER BY city_ref DESC, city_ref_city_label DESC
