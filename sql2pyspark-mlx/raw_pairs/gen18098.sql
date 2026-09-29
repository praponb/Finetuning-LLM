SELECT p.id, p.stock_qty, t.last_last_restock
FROM products p
INNER JOIN (SELECT product_id, MAX(last_restock) AS last_last_restock
    FROM inventory
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY t.last_last_restock DESC, stock_qty DESC, id ASC
LIMIT 5
