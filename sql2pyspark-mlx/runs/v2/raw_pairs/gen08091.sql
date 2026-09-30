SELECT id, last_restock, quantity FROM inventory WHERE last_restock <= '2023-04-02' AND (quantity >= 176 OR product_id IN (1, 2, 9, 10));
