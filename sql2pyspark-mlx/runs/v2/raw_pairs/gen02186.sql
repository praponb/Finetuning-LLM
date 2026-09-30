SELECT
    status,
    QUARTER(paid_at) AS paid_quarter,
    TO_DATE(paid_at) AS paid_day,
    ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refunded
FROM payments
GROUP BY status, paid_quarter, paid_day
HAVING ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 47;
