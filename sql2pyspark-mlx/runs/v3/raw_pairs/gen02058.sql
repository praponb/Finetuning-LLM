SELECT *
FROM inventory
WHERE quantity BETWEEN 77 AND 104 AND product_id IN (3, 4, 6, 8);
