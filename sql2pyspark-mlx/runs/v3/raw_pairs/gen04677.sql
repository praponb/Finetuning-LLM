SELECT
    p.launch_date,
    p.cost,
    p.stock_qty,
    oi.unit_price,
    oi.product_id,
    oi.quantity
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
WHERE p.brand IS NOT NULL;
