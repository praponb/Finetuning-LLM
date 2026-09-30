SELECT
    id,
    monthly_fee,
    end_date,
    COALESCE(monthly_fee, 0) AS monthly_fee_filled,
    CEIL(monthly_fee) AS monthly_fee_whole
FROM subscriptions
WHERE status IN ('PAUSED', 'ACTIVE', 'CANCELLED') AND (plan = 'standard' OR end_date <= '2024-11-01')
ORDER BY id DESC
LIMIT 20;
