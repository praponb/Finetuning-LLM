SELECT method, COUNT(amount) AS amount_count, MAX(amount) - MIN(amount) AS amount_range FROM payments GROUP BY method;
