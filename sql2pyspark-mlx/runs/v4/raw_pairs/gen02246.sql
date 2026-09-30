SELECT
    id,
    customer_id,
    start_date,
    status
FROM (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY start_date DESC) AS rnk
    FROM subscriptions
) AS ranked
WHERE rnk <= 3
ORDER BY start_date DESC, customer_id DESC;
