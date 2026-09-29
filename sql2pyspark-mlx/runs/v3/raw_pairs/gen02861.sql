SELECT COUNT(*) total_count, ROUND((100 * SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_paused FROM subscriptions WHERE status = 'EXPIRED';
