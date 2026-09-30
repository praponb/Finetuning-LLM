SELECT method, MAX(amount) - MIN(amount) AS amount_range FROM payments GROUP BY method HAVING MAX(amount) - MIN(amount) > 60 ORDER BY 1 LIMIT 50;
