SELECT
    id,
    monthly_fee,
    end_date,
    status
FROM subscriptions
WHERE customer_id IN (4, 15, 24, 26) AND customer_id = 9;
