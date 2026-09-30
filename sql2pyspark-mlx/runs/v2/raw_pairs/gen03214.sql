SELECT
    p.category,
    p.product_name,
    t.max_unit_price,
    t.revenue
FROM products p
LEFT OUTER JOIN (SELECT product_id, MAX(unit_price) AS max_unit_price, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id
