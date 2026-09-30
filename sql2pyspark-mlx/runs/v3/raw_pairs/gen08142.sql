SELECT
    id,
    status,
    monthly_fee,
    end_date
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY monthly_fee) AS rnk
    FROM subscriptions
) AS r
WHERE rnk = 1
ORDER BY end_date DESC, status DESC
