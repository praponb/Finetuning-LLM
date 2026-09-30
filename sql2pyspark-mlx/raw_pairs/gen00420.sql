SELECT s.sqft, i.last_restock, i.quantity, i.product_id FROM stores s LEFT JOIN inventory i ON i.store_id = s.id WHERE s.opened_date <= '2023-08-09' AND YEAR(s.opened_date) >= 2024;
