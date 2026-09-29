WITH valid AS (
    SELECT
        *,
        FLOOR(sqft) AS sqft_whole,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM stores
    WHERE opened_date BETWEEN '2024-06-01' AND '2024-12-01'
),
with_latest AS (
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.opened_date,
        s.sqft,
        top1.store_sale_sale_date
    FROM valid s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
remaining_keys AS (
    SELECT id, region
    FROM eligible
    EXCEPT
    SELECT id, region
    FROM stores
    WHERE region IN ('South', 'North', 'Central')
),
kept_rows AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.region = k.region
),
metrics AS (
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        MIN(sqft) OVER (PARTITION BY region) AS region_min_sqft
    FROM kept_rows
    WHERE store_sale_sale_date BETWEEN '2023-07-15' AND '2025-02-06'
),
cleaned AS (
    SELECT
        *,
        sqft / NULLIF(sqft_share, 0) AS sqft_per_sqft_share,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM metrics
    WHERE region IN ('Central', 'South', 'North')
)
SELECT
    id,
    store_name,
    sqft,
    sqft_share,
    region_min_sqft,
    sqft_per_sqft_share,
    resolved_region,
    CASE WHEN store_name RLIKE '^(Mall Central|Siam Flagship)$' THEN 'OTHER_ZONE' WHEN store_name RLIKE '^(Downtown Express|Airport Kiosk)$' THEN 'GROUP_A' ELSE 'CORE' END AS store_name_zone
FROM cleaned
ORDER BY store_name_zone ASC NULLS LAST;
