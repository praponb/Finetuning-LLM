WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months,
        SUBSTRING(city, 1, 4) AS city_prefix
    FROM stores
    WHERE store_name LIKE 'Old%'
      AND region IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (4, 6)
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT city
    FROM kept
    EXCEPT
    SELECT city
    FROM stores
    WHERE region = 'East'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN matched_keys k ON s.city = k.city
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND MONTH(ch.order_date) IN (1, 6, 11)
        )
),
kept_keys AS (
    -- Set intersection on keys
    SELECT region, id
    FROM eligible
    INTERSECT ALL
    SELECT region, id
    FROM stores
    WHERE city = 'Singapore'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.region = k.region AND s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        opened_date_plus_months,
        city_prefix,
        DATE_TRUNC('year', opened_date_plus_months) AS opened_date_plus_months_period
    FROM verified
    WHERE city = 'London'
)
SELECT
    id,
    city,
    region,
    opened_date_plus_months,
    city_prefix,
    opened_date_plus_months_period
FROM valid
ORDER BY city DESC NULLS LAST, region;
