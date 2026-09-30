SELECT p.supplier_id, t.avg_quantity, t.total_count
FROM products p
JOIN (SELECT product_id, AVG(quantity) AS avg_quantity, COUNT(*) AS total_count
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id;
