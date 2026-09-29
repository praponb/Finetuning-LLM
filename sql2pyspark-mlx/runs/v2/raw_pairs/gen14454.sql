SELECT id, amount, status, method, TO_DATE(paid_at) paid_at_date FROM payments WHERE NOT (paid_at < '2023-09-01' AND HOUR(paid_at) BETWEEN 11 AND 17);
