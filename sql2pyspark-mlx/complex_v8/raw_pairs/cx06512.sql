WITH base AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown
    FROM warehouses
    WHERE opened_on < DATE '2025-07-03'
)
SELECT city_or_unknown, id, opened_on AS day
FROM base
UNION
SELECT city, id, opened_on
FROM warehouses
ORDER BY city_or_unknown
