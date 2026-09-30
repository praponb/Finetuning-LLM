WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        INITCAP(TRIM(store_name)) AS store_name_title,
        COALESCE(region, 'OTHER') AS region_clean
    FROM stores
    WHERE sqft >= 9740
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        store_name_title,
        CONCAT(region, ' - ', store_name) AS region_store_name,
        CONCAT(store_name, ' - ', region) AS store_name_region,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM cleaned
    WHERE opened_date > DATE '2023-05-09'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        store_name_title,
        region_store_name,
        store_name_region,
        resolved_region,
        LAG(sqft, 1) OVER (PARTITION BY region ORDER BY sqft, id) AS prev_sqft,
        MAX(sqft) OVER (PARTITION BY region) AS region_max_sqft,
        NTILE(5) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2
    FROM filtered
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_avg_2 DESC NULLS FIRST, id) = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.opened_date
        )
)
-- Window metrics over several different partitions
SELECT
    id,
    store_name,
    region,
    opened_date,
    sqft,
    store_name_title,
    region_store_name,
    store_name_region,
    resolved_region,
    prev_sqft,
    region_max_sqft,
    sqft_bucket,
    sqft_share,
    sqft_avg_2,
    SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
    LAST_VALUE(sqft_avg_2) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft_avg_2
FROM eligible
