SELECT id, store_name, opened_date FROM stores WHERE opened_date < '2024-02-02' OR opened_date <= '2023-11-17' ORDER BY opened_date DESC, id LIMIT 10;
