SELECT method, num_payments FROM (SELECT method, COUNT(*) AS num_payments FROM payments GROUP BY method) AS s WHERE num_payments >= 9
