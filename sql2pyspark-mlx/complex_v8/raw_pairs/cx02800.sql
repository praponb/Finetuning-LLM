WITH scoped_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        SUBSTRING(TRIM(wh_code), 1, 3) AS wh_code_prefix
    FROM warehouses
    WHERE city = 'Kuala Lumpur'
      AND LOWER(wh_code) LIKE '%k1%'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY city), 0), 4) AS capacity_share_of_city,
        NTILE(10) OVER (PARTITION BY city ORDER BY opened_on, id) AS opened_on_decile,
        LAST_VALUE(opened_on) OVER (PARTITION BY city ORDER BY opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_on,
        ROUND(capacity - AVG(capacity) OVER w_city, 2) AS capacity_vs_city_avg
    FROM scoped_warehouses
    WINDOW w_city AS (PARTITION BY city)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        wh_code_prefix,
        opened_on_decile,
        last_opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown,
        DATE_TRUNC('MONTH', last_opened_on) AS last_opened_on_month
    FROM sequenced
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, capacity, opened_on_decile
    FROM enriched
) src
UNPIVOT (
    reading FOR measure IN (capacity, opened_on_decile)
) unp
