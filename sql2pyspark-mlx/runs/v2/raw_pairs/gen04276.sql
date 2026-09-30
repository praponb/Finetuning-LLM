SELECT ROUND(AVG(quantity), 2) AS avg_quantity FROM inventory WHERE product_id IN (1, 3, 9, 10) AND last_restock <= '2024-12-01' ORDER BY avg_quantity DESC;
