SELECT
    p.id,
    p.price,
    t.stddev_unit_price,
    t.unique_products
FROM products p
LEFT JOIN (SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price, COUNT(DISTINCT quantity) AS unique_products
    FROM order_items
    WHERE order_id = 8
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY price DESC, t.stddev_unit_price
