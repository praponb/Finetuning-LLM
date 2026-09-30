SELECT
    products.id,
    products.supplier_id,
    t.max_quantity,
    t.quantity_range
FROM products
LEFT OUTER JOIN (SELECT product_id, MAX(quantity) AS max_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    WHERE order_id IN (4, 16, 33, 36)
    GROUP BY product_id) t ON products.id = t.product_id;
