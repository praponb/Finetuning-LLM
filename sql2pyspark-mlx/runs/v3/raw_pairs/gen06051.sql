SELECT
    id,
    customer_id,
    monthly_fee,
    start_date
FROM (
    SELECT
        id,
        customer_id,
        monthly_fee,
        start_date,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) AS row_num
    FROM subscriptions
    WHERE customer_id IS NULL
) AS ranked
WHERE row_num <= 3
