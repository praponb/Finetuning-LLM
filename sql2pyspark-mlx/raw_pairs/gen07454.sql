SELECT id, customer_id, end_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS row_num
    FROM subscriptions
) AS ranked
WHERE row_num = 1
