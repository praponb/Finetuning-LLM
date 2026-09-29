WITH matched_keys AS (
    -- Set intersection on keys
    SELECT id, region
    FROM stores
    INTERSECT ALL
    SELECT id, region
    FROM stores
    WHERE store_name LIKE 'Ma%'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    JOIN matched_keys k ON s.id = k.id AND s.region = k.region
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date < s.opened_date
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_running AS sqft_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
    FROM eligible
    WINDOW w_running AS (PARTITION BY city ORDER BY sqft NULLS LAST)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.product_id IN (1, 8, 9)
        )
)
-- Ranking and gap features
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    sqft_rank,
    sqft_share,
    RANK() OVER (PARTITION BY region ORDER BY sqft_rank NULLS FIRST) AS sqft_rank_rank,
    STDDEV_SAMP(sqft_rank) OVER (PARTITION BY region) AS region_stddev_samp_sqft_rank,
    CUME_DIST() OVER (ORDER BY sqft DESC) AS sqft_cume_dist
FROM kept;
