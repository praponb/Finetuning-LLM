SELECT * FROM (SELECT status, MIN(amount) AS min_amount FROM payments GROUP BY status) AS agg WHERE min_amount > 1250 ORDER BY status DESC;
