WITH enriched AS (
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date), status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
    FROM accounts
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.status,
    s.running_balance,
    s.first_known_status,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM enriched s
ORDER BY customer_id DESC;
