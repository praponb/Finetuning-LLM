SELECT
    id,
    monthly_fee,
    DATE_TRUNC('YEAR', end_date) AS end_date_period,
    DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
FROM subscriptions
WHERE monthly_fee BETWEEN 180 AND 230 AND (plan IN ('basic', 'enterprise', 'standard') OR status IN ('ACTIVE', 'EXPIRED'));
