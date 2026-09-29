WITH with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.brand,
        s.cost,
        s.launch_date,
        top1.order_item_quantity,
        top1.unit_price
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    CASE WHEN unit_price >= 360 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_tier,
    COUNT(*) FILTER (WHERE brand = 'Initech') AS initech_rows,
    SUM(CASE WHEN brand = 'Globex' THEN unit_price ELSE 0 END) AS globex_unit_price,
    SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) AS acme_cost,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count
FROM with_latest
GROUP BY CUBE (DATE_TRUNC('QUARTER', launch_date), CASE WHEN unit_price >= 360 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY initech_rows DESC NULLS LAST
