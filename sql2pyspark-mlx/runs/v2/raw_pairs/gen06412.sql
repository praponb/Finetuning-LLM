SELECT p.brand, COUNT(*) cnt, COUNT(*) num_inventory
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.product_name LIKE 'Wa%'
GROUP BY p.brand
ORDER BY brand DESC;
