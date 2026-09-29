SELECT HOUR(paid_at) AS paid_hour, COUNT(DISTINCT amount) AS unique_orders FROM payments GROUP BY HOUR(paid_at) ORDER BY 2 DESC;
