WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        region,
        sqft
    FROM stores
    QUALIFY RANK() OVER (PARTITION BY city, region ORDER BY opened_date DESC) = 1
)
SELECT
    id,
    city,
    sqft
FROM latest
ORDER BY id DESC
LIMIT 5;
