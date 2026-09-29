WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN quantity >= 270 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    MIN(quantity) AS min_quantity,
    PERCENTILE_APPROX(quantity, 0.5) AS quantity_p50,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95
FROM latest
GROUP BY CASE WHEN quantity >= 270 THEN 'LARGE' ELSE 'SMALL' END
HAVING min_quantity >= 0;
