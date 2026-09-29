SELECT
    method,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
    SUM(CASE WHEN method = 'card' THEN amount ELSE 0 END) AS card_amount,
    MIN(amount) AS min_amount
FROM payments
GROUP BY method
ORDER BY card_amount ASC, method ASC
LIMIT 10;
