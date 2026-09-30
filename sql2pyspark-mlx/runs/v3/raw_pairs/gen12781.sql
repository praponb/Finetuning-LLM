SELECT id, quantity FROM inventory WHERE last_restock BETWEEN '2025-03-15' AND '2025-10-01' EXCEPT SELECT id, quantity FROM inventory WHERE YEAR(last_restock) = 2025;
