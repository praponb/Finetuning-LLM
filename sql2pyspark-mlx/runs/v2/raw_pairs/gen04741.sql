SELECT id, amount, method, status FROM payments WHERE HOUR(paid_at) BETWEEN 10 AND 13 ORDER BY amount DESC;
