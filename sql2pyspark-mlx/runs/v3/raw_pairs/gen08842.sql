SELECT HOUR(paid_at) AS paid_hour, COUNT(amount) AS amount_count, MAX(amount) - MIN(amount) AS amount_range FROM payments GROUP BY HOUR(paid_at);
