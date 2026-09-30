WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    PERCENTILE_APPROX(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    SUM(element) AS total_element,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(element, 0.5) AS element_p50,
    COUNT(*) AS cnt,
    ROUND(AVG(element), 2) AS avg_element
FROM exploded
GROUP BY 1;
