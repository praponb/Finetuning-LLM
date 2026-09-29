SELECT * FROM (SELECT status, COUNT(amount) AS amount_count FROM payments WHERE status != 'FAILED' GROUP BY status) AS agg WHERE amount_count >= 7 ORDER BY amount_count DESC, status;
