SELECT id, customer_id, end_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS rnk
    FROM subscriptions
) AS r
WHERE rnk = 1
ORDER BY id DESC, customer_id DESC;
