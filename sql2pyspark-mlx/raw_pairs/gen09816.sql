SELECT s.id, s.city, s.store_name, t.amount_count FROM stores s JOIN (SELECT store_id, COUNT(amount) AS amount_count FROM orders GROUP BY store_id) t ON s.id = t.store_id ORDER BY amount_count DESC
