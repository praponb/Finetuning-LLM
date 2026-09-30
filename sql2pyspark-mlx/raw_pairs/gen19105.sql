WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DAYOFWEEK(opened_date) AS opened_date_dayofweek,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(city) AS city_length,
        SUBSTRING(city, 1, 3) AS city_prefix
    FROM cleaned
    WHERE opened_date > DATE '2023-05-11'
      AND sqft BETWEEN 5112 AND 11670
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock < '2025-04-01'
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date < s.opened_date
        )
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.store_sale_units,
        x.revenue
    FROM eligible s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    city,
    region,
    opened_date_dayofweek,
    city_length,
    city_prefix,
    revenue,
    CASE WHEN region RLIKE '^(West|East)$' THEN 'OTHER_ZONE' WHEN region RLIKE '^(North|South)$' THEN 'CORE' ELSE 'GROUP_A' END AS region_zone
FROM with_latest;
