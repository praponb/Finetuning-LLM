SELECT store_id, COALESCE(quantity, 0) quantity_filled
FROM inventory
WHERE product_id IN (3, 4, 9, 10) AND quantity >= 210;
