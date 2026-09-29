SELECT method, MAX(amount) AS max_amount, MAX(amount) - MIN(amount) AS amount_range FROM payments GROUP BY method HAVING MAX(amount) >= 1540;
