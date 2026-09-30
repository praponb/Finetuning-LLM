SELECT status, SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) AS paused_count
FROM subscriptions
WHERE plan = 'premium' AND plan = 'premium'
GROUP BY status
ORDER BY paused_count DESC, status
