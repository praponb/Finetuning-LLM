SELECT
    id,
    customer_id,
    start_date,
    status
FROM (
    SELECT
        id,
        customer_id,
        start_date,
        status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS rn
    FROM subscriptions
) AS t
WHERE rn <= 5
ORDER BY customer_id, 1 DESC;
