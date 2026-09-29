SELECT *, WEEKOFYEAR(last_restock) last_restock_weekofyear
FROM inventory
WHERE quantity >= 162;
