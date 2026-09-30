SELECT i.last_restock, s.store_name, p.brand
FROM inventory i
RIGHT JOIN stores s ON s.id = i.store_id
INNER JOIN products p ON i.product_id = p.id
WHERE s.opened_date > '2024-07-01' AND s.sqft = 7802;
