SELECT o.store_id, t.avg_quantity, t.num_order_items
FROM orders o
JOIN (SELECT order_id, AVG(quantity) AS avg_quantity, COUNT(*) AS num_order_items
    FROM order_items
    WHERE product_id IN (4, 9, 15, 19)
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY num_order_items;
