SELECT
    id,
    customer_id,
    end_date,
    monthly_fee
FROM (
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY end_date DESC) AS rn
    FROM subscriptions
) AS r
WHERE rn = 1
