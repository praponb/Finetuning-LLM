SELECT p.id, p.product_name, t.total_quantity
FROM products p
JOIN (SELECT product_id, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE last_restock < '2023-10-01'
    GROUP BY product_id) t ON p.id = t.product_id;
