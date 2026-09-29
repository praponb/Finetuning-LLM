WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(email, 1, 3) AS email_prefix,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM customers
    WHERE signup_date > DATE '2023-07-05'
)
-- Ranking and gap features
SELECT
    *,
    AVG(credit_limit) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), city) AS signup_date_period_avg_credit_limit,
    NTILE(4) OVER (ORDER BY credit_limit DESC, id) AS credit_limit_bucket
FROM valid
WHERE city IN ('Singapore', 'London', 'New York');
