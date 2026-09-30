SELECT products.launch_date, t.avg_quantity, t.quantity_count
FROM products
LEFT JOIN (SELECT product_id, AVG(quantity) avg_quantity, COUNT(quantity) quantity_count
    FROM order_items
    WHERE unit_price >= 210
    GROUP BY product_id) t ON products.id = t.product_id
ORDER BY avg_quantity DESC;
