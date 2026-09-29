SELECT o.amount, t.unique_products
FROM orders o
INNER JOIN (SELECT order_id, COUNT(DISTINCT unit_price) AS unique_products
    FROM order_items
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY unique_products DESC;
