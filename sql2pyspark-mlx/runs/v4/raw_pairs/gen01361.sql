WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC NULLS LAST, id ASC) AS latest_rank
    FROM accounts
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.status,
    s.latest_rank,
    CASE WHEN status RLIKE '^(FROZEN|OPEN)$' THEN 'EXTENDED' WHEN status RLIKE '^(CLOSED)$' THEN 'GROUP_B' ELSE 'CORE' END AS status_zone,
    (SELECT MIN(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM ranked s
ORDER BY customer_id DESC NULLS LAST, id DESC
