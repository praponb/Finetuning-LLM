WITH with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.store_sale_units
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT city, region
    FROM with_latest
    EXCEPT DISTINCT
    SELECT city, region
    FROM stores
    WHERE store_name LIKE '%Kios%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN kept_keys k ON s.city = k.city AND s.region = k.region
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(store_sale_units / SUM(store_sale_units) OVER (PARTITION BY region), 4) AS store_sale_units_share,
        AVG(sqft) OVER (PARTITION BY region) AS region_avg_sqft
    FROM reconciled
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.quantity <= 221
        )
)
-- Grouped metrics
SELECT
    CASE WHEN store_sale_units_share >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END AS store_sale_units_share_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    SUM(CASE WHEN region = 'Central' THEN sqft ELSE 0 END) AS central_sqft,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles_2,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(DISTINCT city) AS distinct_cities,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN store_sale_units_share >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT city) DESC) AS distinct_cities_dense_rank
FROM not_flagged
GROUP BY 1, 2;
