WITH with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.units,
        top1.store_sale_revenue
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.units, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    city,
    CASE WHEN store_sale_revenue >= 3900 THEN 'HIGH' ELSE 'LOW' END AS store_sale_revenue_tier,
    region,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM with_latest
GROUP BY city, store_sale_revenue_tier, region
ORDER BY city NULLS LAST;
