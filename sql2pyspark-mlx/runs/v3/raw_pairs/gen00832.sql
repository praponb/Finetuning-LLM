SELECT status, COUNT(*) AS total_count, SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee ELSE 0 END) AS active_monthly_fee
FROM subscriptions
WHERE status NOT IN ('PAUSED', 'EXPIRED') AND start_date > '2024-03-01'
GROUP BY status
ORDER BY 2 DESC;
