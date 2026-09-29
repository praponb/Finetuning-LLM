SELECT products.brand, products.price, t.stddev_quantity
FROM products
INNER JOIN (SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    GROUP BY product_id) t ON products.id = t.product_id
WHERE t.stddev_quantity > 50
