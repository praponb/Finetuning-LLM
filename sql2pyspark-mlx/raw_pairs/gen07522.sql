WITH with_latest AS (
    SELECT
        s.*,
        top1.unit_price,
        top1.quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    s.unit_price,
    CASE WHEN brand RLIKE '^(Stark|Globex)$' THEN 'GROUP_B' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS brand_zone,
    (SELECT AVG(p2.stock_qty) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS avg_product_stock_qty
FROM with_latest s
ORDER BY stock_qty DESC NULLS LAST
