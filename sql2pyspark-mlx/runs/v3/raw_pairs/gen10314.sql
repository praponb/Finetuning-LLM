SELECT
    id,
    monthly_fee,
    end_date,
    status
FROM subscriptions
WHERE monthly_fee BETWEEN 210 AND 230 AND monthly_fee >= 140
ORDER BY status;
