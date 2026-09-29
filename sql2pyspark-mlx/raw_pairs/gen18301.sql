SELECT last_restock FROM inventory WHERE quantity != 75 UNION SELECT last_restock FROM inventory WHERE quantity != 111 ORDER BY last_restock
