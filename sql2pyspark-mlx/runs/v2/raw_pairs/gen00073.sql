SELECT
    id,
    customer_id,
    end_date,
    start_date
FROM (
    SELECT
        id,
        customer_id,
        end_date,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS rn
    FROM subscriptions
) AS r
WHERE rn = 1
