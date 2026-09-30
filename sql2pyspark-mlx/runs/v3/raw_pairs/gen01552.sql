SELECT p.category, p.price, t.revenue
FROM products p
INNER JOIN (SELECT product_id, SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY revenue DESC;
