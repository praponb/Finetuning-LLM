SELECT method, MAX(amount) AS max_amount FROM payments GROUP BY method ORDER BY max_amount DESC
