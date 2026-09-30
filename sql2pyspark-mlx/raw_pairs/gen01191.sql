SELECT
    src.*,
    sub.id AS subscription_id,
    sub.plan,
    sub.monthly_fee
FROM sessions src
INNER JOIN subscriptions sub
    ON src.customer_id = sub.customer_id
   AND src.started_at >= sub.start_date
   AND src.started_at <= DATE_ADD(sub.end_date, 3);
