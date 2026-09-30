SELECT
    products.category,
    order_items.order_id,
    order_items.quantity,
    order_items.unit_price
FROM products
INNER JOIN order_items ON products.id = order_items.product_id
WHERE order_items.order_id IN (15, 16, 38) AND products.stock_qty <= 198
ORDER BY order_items.unit_price ASC;
