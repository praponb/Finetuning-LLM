WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        sqft,
        ROUND(sqft, 2) AS sqft_rounded
    FROM stores
    WHERE opened_date > DATE '2023-06-20'
      AND region IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.revenue BETWEEN 1400 AND 3000
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM kept
    INTERSECT
    SELECT id
    FROM stores
    WHERE LENGTH(store_name) < 13
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN matched_keys k ON s.id = k.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.product_id = 10
        )
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT region, id
    FROM eligible
    INTERSECT
    SELECT region, id
    FROM stores
    WHERE region IN ('Central', 'South', 'West')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys_2 k ON s.region = k.region AND s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.status NOT IN ('SHIPPED', 'CANCELLED', 'PENDING')
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN sqft >= 14937 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    BIT_AND(sqft) AS sqft_bit_and,
    ROUND(AVG(sqft_rounded), 2) AS avg_sqft_rounded,
    COUNT_IF(region = 'West') AS west_events,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(*) FILTER (WHERE region = 'South') AS south_rows,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM not_flagged
GROUP BY CASE WHEN sqft >= 14937 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY avg_sqft_rounded DESC;
