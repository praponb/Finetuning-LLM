WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date < s.opened_date
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date <= s.opened_date
        )
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.sqft,
        top1.shipping_fee
    FROM eligible s
    JOIN LATERAL (
            SELECT c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT region, id
    FROM latest_match
    INTERSECT ALL
    SELECT region, id
    FROM stores
    WHERE sqft <> 13134
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    JOIN remaining_keys k ON s.region = k.region AND s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        region,
        shipping_fee,
        CUME_DIST() OVER (ORDER BY shipping_fee NULLS LAST) AS shipping_fee_cume_dist,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share
    FROM kept_rows
),
eligible_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.quantity < 127
        )
)
SELECT
    region,
    shipping_fee,
    shipping_fee_cume_dist,
    sqft_share,
    CASE WHEN region RLIKE '^(Central|East)$' THEN 'CORE' WHEN region RLIKE '^(North|South)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS region_zone
FROM eligible_stores
ORDER BY region DESC NULLS FIRST
