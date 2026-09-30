WITH base AS (
    SELECT
        id,
        city,
        region,
        sqft,
        UPPER(city) AS city_upper
    FROM stores
    WHERE sqft > 9234
      AND region IS NOT NULL
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM orders x
        WHERE x.store_id = s.id
          AND x.order_date < '2024-02-01'
    )
