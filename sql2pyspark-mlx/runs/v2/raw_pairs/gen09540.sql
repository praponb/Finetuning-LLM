SELECT status, min_amount FROM (SELECT status, MIN(amount) AS min_amount FROM orders GROUP BY status) AS s WHERE min_amount > 800 ORDER BY status LIMIT 5
