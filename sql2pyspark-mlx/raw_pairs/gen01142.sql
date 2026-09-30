SELECT orders.amount, t.revenue, t.unique_products
FROM orders
INNER JOIN (SELECT order_id, SUM(quantity * unit_price) revenue, COUNT(DISTINCT quantity) unique_products
    FROM order_items
    WHERE quantity IS NOT NULL
    GROUP BY order_id) t ON orders.id = t.order_id
