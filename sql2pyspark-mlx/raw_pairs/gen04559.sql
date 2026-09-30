WITH exploded AS (
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS element
)
SELECT
    CASE WHEN element >= 77 THEN 'HIGH' ELSE 'LOW' END AS element_tier,
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count,
    ROUND(AVG(element), 2) AS avg_element,
    GROUPING(CASE WHEN element >= 77 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM exploded
GROUP BY CUBE (CASE WHEN element >= 77 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', effective_from))
HAVING ROUND(AVG(element), 2) < 1000000
