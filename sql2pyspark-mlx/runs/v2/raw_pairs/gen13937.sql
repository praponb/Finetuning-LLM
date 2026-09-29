SELECT method, ROUND((100 * SUM(CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bank_transfer, MAX(amount) AS max_amount FROM payments GROUP BY method;
