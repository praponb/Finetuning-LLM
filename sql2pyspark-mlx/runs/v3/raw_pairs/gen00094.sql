SELECT *, HOUR(paid_at) AS paid_at_hour FROM payments ORDER BY paid_at, id LIMIT 20;
