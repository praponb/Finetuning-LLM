SELECT
    id,
    status,
    monthly_fee,
    RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC) AS rnk
FROM subscriptions
WHERE customer_id IN (18, 25, 31, 40);
