SELECT id, quantity, product_id
FROM inventory
WHERE quantity >= (SELECT MAX(quantity) FROM inventory)
ORDER BY product_id DESC;
