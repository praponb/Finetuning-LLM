WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        pos,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) t AS pos, line
)
-- Multi-level aggregation
SELECT
    CASE WHEN pos >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS pos_tier,
    COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END) AS bundle_count,
    ROUND(AVG(qty), 2) AS avg_qty
FROM exploded
GROUP BY CASE WHEN pos >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END;
