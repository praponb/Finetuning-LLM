SELECT order_id, MIN(amount) AS min_amount, COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count FROM payments GROUP BY order_id ORDER BY order_id DESC;
