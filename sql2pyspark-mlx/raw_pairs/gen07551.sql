WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock <= s.opened_date
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    SIZE(COLLECT_SET(region)) AS num_regions
FROM not_flagged
GROUP BY DATE_TRUNC('WEEK', opened_date)
ORDER BY sqft_stddev DESC
