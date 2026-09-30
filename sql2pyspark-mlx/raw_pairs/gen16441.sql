SELECT p.supplier_id, t.quantity_count, t.unique_stores
FROM products p
LEFT JOIN (SELECT product_id, COUNT(quantity) AS quantity_count, COUNT(DISTINCT quantity) AS unique_stores
    FROM inventory
    WHERE last_restock <= '2024-11-01'
    GROUP BY product_id) t ON p.id = t.product_id
