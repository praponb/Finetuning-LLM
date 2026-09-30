SELECT p.price, t.total_quantity
FROM products p
INNER JOIN (SELECT product_id, SUM(quantity) total_quantity
    FROM inventory
    WHERE product_id IN (2, 4, 6, 7)
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY 1 DESC
