SELECT o.id, o.amount, t.revenue
FROM orders o
LEFT OUTER JOIN (SELECT order_id, SUM(unit_price * quantity) AS revenue
    FROM order_items
    WHERE unit_price < 230
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY amount DESC;
