SELECT method, MAX(paid_at) last_paid_at
FROM payments
GROUP BY method
ORDER BY last_paid_at DESC;
