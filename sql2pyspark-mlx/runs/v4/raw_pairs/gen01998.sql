SELECT last_restock FROM inventory INTERSECT SELECT order_date FROM orders ORDER BY last_restock;
