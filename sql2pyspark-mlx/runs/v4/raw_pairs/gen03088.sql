SELECT
    e.*,
    sub.id AS subscription_id,
    sub.plan,
    sub.monthly_fee
FROM accounts e
LEFT JOIN subscriptions sub
    ON e.customer_id = sub.customer_id
   AND e.opened_date >= sub.start_date
   AND e.opened_date < sub.end_date + INTERVAL 30 DAYS;
