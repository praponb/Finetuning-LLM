SELECT
    p.id,
    p.price,
    p.supplier_id,
    t.max_quantity,
    t.quantity_range
FROM products p
INNER JOIN (SELECT product_id, MAX(quantity) AS max_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    WHERE product_id IN (35, 38)
    GROUP BY product_id) t ON p.id = t.product_id;
