SELECT method, SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END) AS card_count, MAX(amount) AS max_amount, SUM(amount) AS total_amount FROM payments GROUP BY method ORDER BY method LIMIT 1
