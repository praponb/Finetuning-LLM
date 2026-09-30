WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(sqft, 0) AS sqft_filled,
        DATE_TRUNC('YEAR', opened_date) AS opened_date_period,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE UPPER(store_name) LIKE '%IRP%'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT region, city
    FROM base
    INTERSECT ALL
    SELECT region, city
    FROM stores
    WHERE sqft BETWEEN 12954 AND 15722
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.region = k.region AND s.city = k.city
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_filled,
        opened_date_period,
        resolved_region,
        sqft * sqft_filled AS sqft_times_sqft_filled,
        TO_DATE(opened_date_period) AS opened_date_period_date
    FROM kept_rows
    WHERE opened_date > DATE '2023-07-01'
      AND region IS DISTINCT FROM 'North'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.store_id IN (2, 3, 4)
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date BETWEEN '2023-04-25' AND '2023-07-01'
        )
),
not_flagged_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.opened_date_period
        )
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    sqft_filled,
    resolved_region,
    sqft_times_sqft_filled,
    CASE WHEN region RLIKE '^(North|East)$' THEN 'OTHER_ZONE' WHEN region RLIKE '^(South|West)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS region_zone
FROM not_flagged_stores
ORDER BY city;
