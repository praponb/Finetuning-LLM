SELECT
    id,
    customer_id,
    end_date,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY end_date DESC) AS dense_rnk
FROM subscriptions;
