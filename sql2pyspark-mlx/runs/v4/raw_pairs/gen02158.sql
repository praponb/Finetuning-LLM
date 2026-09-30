SELECT p.id, p.category, t.revenue
FROM products p
INNER JOIN (SELECT product_id, SUM(quantity * unit_price) revenue
    FROM order_items
    WHERE unit_price IS NOT NULL
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.revenue >= 76;
