WITH ranked_subscriptions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS rnk
    FROM subscriptions
)
SELECT *
FROM ranked_subscriptions
WHERE rnk <= 1;
