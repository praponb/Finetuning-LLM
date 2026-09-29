WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
)
-- Grouped metrics
SELECT
    CASE WHEN val >= 86 THEN 'HIGH' ELSE 'LOW' END AS val_tier,
    COUNT(*) AS num_rows,
    SUM(val) AS total_val,
    ROUND(AVG(val), 2) AS avg_val
FROM exploded
GROUP BY 1
HAVING num_rows < 1
