WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        SUBSTRING(TRIM(city), 1, 4) AS city_prefix,
        ABS(capacity - 11506) AS capacity_distance
    FROM warehouses
    WHERE YEAR(opened_on) = 2024
),
unique_rows AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_prefix,
        capacity_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on DESC, id DESC) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        city,
        SUM(capacity) AS total_capacity,
        ROUND(AVG(capacity), 2) AS avg_capacity,
        SUM(capacity_distance) AS total_capacity_distance
    FROM unique_rows
    GROUP BY DATE_TRUNC('MONTH', opened_on), city
    HAVING SUM(capacity) > 4912
)
SELECT opened_on_month, city, total_capacity_distance
FROM totals
WHERE total_capacity_distance >= 30000
ORDER BY total_capacity_distance NULLS LAST
