SELECT quantity FROM inventory WHERE quantity < 228 UNION ALL SELECT quantity FROM inventory WHERE last_restock >= '2025-04-01';
