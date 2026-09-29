SELECT id, customer_id, end_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) AS rn
    FROM subscriptions
    WHERE monthly_fee < 140
) AS r
WHERE rn <= 5
ORDER BY customer_id DESC;
