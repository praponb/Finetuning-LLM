SELECT
    id,
    end_date,
    monthly_fee,
    status
FROM subscriptions
WHERE status NOT IN ('PAUSED', 'EXPIRED') AND start_date BETWEEN '2024-03-13' AND '2025-10-19' AND status IN ('ACTIVE', 'PAUSED', 'EXPIRED')
ORDER BY end_date
