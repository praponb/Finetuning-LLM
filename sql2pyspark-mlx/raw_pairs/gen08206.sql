SELECT
    p.id,
    p.launch_date,
    t.unique_products,
    t.latest_last_restock
FROM products p
INNER JOIN (SELECT product_id, COUNT(DISTINCT quantity) AS unique_products, MAX(last_restock) AS latest_last_restock
    FROM inventory
    WHERE store_id IN (1, 5, 6)
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.unique_products >= 6
ORDER BY unique_products DESC, id DESC
LIMIT 5
