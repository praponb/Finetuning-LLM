WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) / 365 AS years_since_opened_date
    FROM stores
    WHERE opened_date > TO_DATE('2023-07-18')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT city
    FROM base
    EXCEPT
    SELECT city
    FROM stores
    WHERE opened_date > '2024-08-25'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.city = k.city
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        years_since_opened_date + sqft AS years_since_opened_date_plus_sqft,
        INITCAP(TRIM(city)) AS city_title
    FROM reconciled
    WHERE region IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= '2025-04-01'
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date >= s.opened_date
        )
)
SELECT
    store_name,
    region,
    opened_date,
    years_since_opened_date,
    years_since_opened_date_plus_sqft,
    CASE WHEN region RLIKE '^(South|West)$' THEN 'PRIORITY' ELSE 'CORE' END AS region_zone
FROM not_flagged
ORDER BY years_since_opened_date NULLS LAST, region_zone DESC;
