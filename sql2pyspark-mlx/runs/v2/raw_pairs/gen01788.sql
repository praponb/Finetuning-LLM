SELECT ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_refunded, MIN(paid_at) first_paid_at
FROM payments
ORDER BY pct_refunded ASC;
