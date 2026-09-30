SELECT id, quantity, last_restock, DATE_TRUNC('week', last_restock) last_restock_period FROM inventory WHERE quantity > 133 ORDER BY id DESC;
