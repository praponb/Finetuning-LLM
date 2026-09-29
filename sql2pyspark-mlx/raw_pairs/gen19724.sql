SELECT
    s.*,
    sub.id AS subscription_id,
    sub.plan AS subscription_plan,
    sub.monthly_fee AS subscription_monthly_fee
FROM subscriptions s
LEFT JOIN subscriptions sub
    ON s.customer_id = sub.customer_id
   AND s.end_date BETWEEN sub.start_date AND sub.end_date;
