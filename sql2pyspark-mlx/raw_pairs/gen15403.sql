WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(region) AS clean_region,
        CASE WHEN region IN ('North', 'Central') THEN 'Tier 1' ELSE 'Other' END AS region_group
    FROM stores
    WHERE opened_date > DATE '2023-04-18'
      AND region IS DISTINCT FROM 'West'
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.shipping_fee
    FROM base s
    JOIN LATERAL (
            SELECT c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY region, city ORDER BY sqft DESC) AS sqft_percent_rank,
        ROW_NUMBER() OVER (ORDER BY sqft, id) AS row_seq
    FROM latest_match
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        sqft,
        sqft_percent_rank,
        SUBSTRING(store_name, 1, 4) AS store_name_prefix,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM enriched
    WHERE opened_date > DATE '2023-09-23'
      AND region IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.store_id = 6
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        region,
        sqft,
        sqft_percent_rank,
        store_name_prefix,
        resolved_region
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY region, city ORDER BY sqft DESC) = 1
)
SELECT
    id,
    region,
    sqft,
    sqft_percent_rank,
    store_name_prefix,
    resolved_region,
    CASE WHEN region RLIKE '^(West|North)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS region_zone
FROM ranked
ORDER BY sqft NULLS FIRST, id DESC
