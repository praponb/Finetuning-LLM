SELECT
    id,
    customer_id,
    end_date,
    status,
    start_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) row_num,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY start_date) dense_rnk
FROM subscriptions
