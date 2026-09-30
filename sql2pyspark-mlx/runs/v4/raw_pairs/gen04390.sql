SELECT *, COALESCE(quantity, 0) quantity_filled, DATE_SUB(last_restock, 72) last_restock_minus_72d
FROM inventory
WHERE product_id = 10
ORDER BY id ASC
LIMIT 10;
