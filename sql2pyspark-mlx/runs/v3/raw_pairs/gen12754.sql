SELECT
    oi.unit_price,
    p.product_name,
    p.category,
    p.stock_qty,
    LOWER(p.category) category_lower
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id;
