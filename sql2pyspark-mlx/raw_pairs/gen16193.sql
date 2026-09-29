WITH with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_item_unit_price
    FROM products s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    brand,
    STDDEV_SAMP(cost) AS cost_stddev,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    MAX(stock_qty) FILTER (WHERE brand = 'Globex') AS globex_max_stock_qty,
    COUNT(*) AS cnt
FROM with_last
GROUP BY 1
ORDER BY stock_qty_bit_or DESC;
