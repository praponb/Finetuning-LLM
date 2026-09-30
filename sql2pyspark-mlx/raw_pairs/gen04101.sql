WITH latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.opened_date,
        s.sqft,
        latest.discount
    FROM stores s
    JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    CASE WHEN sqft >= 13200 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    region,
    PERCENTILE_APPROX(sqft, 0.5) AS sqft_p50,
    AVG(discount) FILTER (WHERE region = 'West') AS west_avg_discount,
    COUNT_IF(region = 'Central') AS central_events
FROM latest_match
GROUP BY DATE_TRUNC('YEAR', opened_date), CASE WHEN sqft >= 13200 THEN 'HIGH' ELSE 'LOW' END, region;
