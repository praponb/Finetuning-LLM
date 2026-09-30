WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN region = 'West' THEN 1 ELSE 0 END AS is_west,
        CONCAT_WS('/', city, region) AS city_region_key,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-08-13'
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_revenue,
        top1.units
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.revenue BETWEEN 2400 AND 2700
        )
),
cleaned_stores AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(TRIM(store_name)) AS store_name_normalized,
        CEIL(store_sale_revenue) AS store_sale_revenue_whole,
        COALESCE(region, 'NONE') AS resolved_region_2
    FROM kept
    WHERE is_west BETWEEN 41 AND 53
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        opened_date,
        is_west,
        resolved_region,
        store_name_normalized,
        store_sale_revenue_whole,
        resolved_region_2
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region, city ORDER BY opened_date DESC NULLS LAST) AS dedup_rank
        FROM cleaned_stores
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    store_name,
    resolved_region,
    store_name_normalized,
    store_sale_revenue_whole,
    resolved_region_2,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Old Town)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS store_name_zone
FROM deduped;
