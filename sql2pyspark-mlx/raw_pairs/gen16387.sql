WITH latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN stock_qty >= 332 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS cost_per_acme,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(*) FILTER (WHERE brand = 'Initech') AS initech_rows,
    SUM(CASE WHEN brand = 'Acme' THEN price ELSE 0 END) AS acme_price,
    PERCENTILE_APPROX(cost, 0.95) AS cost_p95,
    SUM(CASE WHEN brand = 'Globex' THEN cost ELSE 0 END) AS globex_cost
FROM latest_match
GROUP BY stock_qty_tier
HAVING cost_per_acme >= 0;
