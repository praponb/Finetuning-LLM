SELECT p.product_name, i.last_restock
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
ORDER BY last_restock ASC;
