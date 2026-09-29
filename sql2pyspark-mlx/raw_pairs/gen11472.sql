WITH valid AS (
    SELECT *, CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS is_active
    FROM subscriptions
    WHERE end_date < '2023-08-01'
)
SELECT
    id,
    customer_id,
    plan,
    status,
    NTILE(3) OVER (PARTITION BY customer_id ORDER BY is_active, id) AS is_active_bucket,
    NTILE(3) OVER (PARTITION BY status, customer_id ORDER BY is_active DESC, id) AS is_active_bucket_2,
    AVG(monthly_fee) OVER (ORDER BY end_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_5_prior
FROM valid
