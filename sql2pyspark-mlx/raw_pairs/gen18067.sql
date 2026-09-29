WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(region, 'NONE') AS resolved_region
    FROM stores
    WHERE city NOT IN ('Singapore', 'New York', 'Berlin')
      AND opened_date >= TO_DATE('2023-10-07')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM cleaned
    EXCEPT ALL
    SELECT id, city
    FROM stores
    WHERE opened_date > '2025-07-06'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.units <> 11
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.opened_date
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.opened_date
        )
)
SELECT
    store_name,
    region,
    opened_date,
    sqft,
    days_since_opened_date,
    resolved_region
FROM kept;
