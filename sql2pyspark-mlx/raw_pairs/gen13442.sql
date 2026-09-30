WITH latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.stock_qty,
        s.launch_date,
        s.supplier_id,
        latest.order_item_unit_price,
        latest.quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    s.id,
    s.category,
    s.supplier_id,
    s.order_item_unit_price,
    (SELECT SUM(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS sum_product_price
FROM latest_match s
ORDER BY supplier_id DESC NULLS FIRST
