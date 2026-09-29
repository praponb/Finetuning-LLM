SELECT status, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled FROM subscriptions WHERE monthly_fee BETWEEN 160 AND 180 GROUP BY status;
