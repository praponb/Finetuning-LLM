SELECT o.discount, t.revenue
FROM orders o
INNER JOIN (SELECT order_id, SUM(unit_price * quantity) AS revenue
    FROM order_items
    WHERE product_id = 33
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.revenue >= 51
ORDER BY o.discount, t.revenue DESC;
