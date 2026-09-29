SELECT
    id,
    status,
    customer_id,
    monthly_fee,
    end_date,
    LAST_VALUE(end_date) OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date
FROM subscriptions
ORDER BY customer_id
