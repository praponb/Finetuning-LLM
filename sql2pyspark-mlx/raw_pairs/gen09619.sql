WITH joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.units,
        e.revenue,
        b.sqft,
        b.region
    FROM store_sales e
    LEFT JOIN stores b
        ON e.store_id = b.id
)
-- Grouped metrics
SELECT
    CASE WHEN sqft >= 15533 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    SUM(CASE WHEN region = 'North' THEN units ELSE 0 END) AS north_units,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    APPROX_PERCENTILE(revenue, 0.95) AS revenue_p95,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    ROUND(SUM(revenue) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS revenue_per_west
FROM joined
GROUP BY CASE WHEN sqft >= 15533 THEN 'HIGH' ELSE 'LOW' END
ORDER BY sqft_tier;
