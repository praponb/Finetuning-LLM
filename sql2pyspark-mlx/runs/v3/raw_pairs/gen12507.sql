SELECT quantity FROM inventory WHERE quantity BETWEEN 62 AND 64 UNION ALL SELECT quantity FROM inventory WHERE last_restock <= '2023-03-01' ORDER BY quantity
