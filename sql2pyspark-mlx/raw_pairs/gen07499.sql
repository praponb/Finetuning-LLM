WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS rank_in_group
    FROM stores
    QUALIFY rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN sqft >= 17078 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(sqft, 0.5) AS sqft_p50,
    MAX(sqft) AS max_sqft
FROM latest
GROUP BY 1, 2
HAVING COUNT(DISTINCT id) > 1
ORDER BY bangkok_rows DESC NULLS LAST
