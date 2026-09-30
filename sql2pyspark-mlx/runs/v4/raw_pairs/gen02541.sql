SELECT COUNT(*) AS cnt FROM inventory WHERE product_id = 4 AND (product_id IN (3, 5, 7) OR YEAR(last_restock) >= 2024) ORDER BY cnt DESC;
