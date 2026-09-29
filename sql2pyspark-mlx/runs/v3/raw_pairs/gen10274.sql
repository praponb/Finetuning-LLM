SELECT
    id,
    end_date,
    customer_id,
    monthly_fee
FROM subscriptions
WHERE end_date <= (SELECT MIN(end_date) FROM subscriptions)
ORDER BY customer_id DESC
