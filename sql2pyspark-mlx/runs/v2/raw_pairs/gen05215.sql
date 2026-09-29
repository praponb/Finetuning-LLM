SELECT
    id,
    customer_id,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) row_num
FROM subscriptions
WHERE status = 'ACTIVE'
