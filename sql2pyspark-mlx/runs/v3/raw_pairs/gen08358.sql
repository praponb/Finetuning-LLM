SELECT id, customer_id, monthly_fee
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS rnk
    FROM subscriptions
) AS t
WHERE rnk = 1;
