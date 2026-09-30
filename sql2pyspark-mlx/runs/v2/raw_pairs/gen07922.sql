SELECT *, COALESCE(monthly_fee, 0) AS monthly_fee_filled, INITCAP(TRIM(status)) AS status_title
FROM subscriptions
WHERE plan <> 'enterprise' AND status NOT IN ('EXPIRED', 'PAUSED')
