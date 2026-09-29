WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        region,
        sqft,
        LAG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS event_seq
    FROM stores
    WHERE sqft < 10365
)
-- Grouped metrics
SELECT
    region,
    BIT_OR(sqft) AS sqft_bit_or,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count,
    COUNT(DISTINCT region) AS distinct_regions,
    SUM(CASE WHEN region = 'West' THEN prev_sqft ELSE 0 END) AS west_prev_sqft,
    APPROX_PERCENTILE(sqft, 0.9) AS sqft_p90,
    COUNT(*) AS row_count
FROM scored
GROUP BY region
HAVING west_prev_sqft >= 0
ORDER BY sqft_bit_or DESC NULLS FIRST
