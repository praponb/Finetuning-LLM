WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id ASC) AS latest_rank
        FROM subscriptions
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.monthly_fee,
    s.status,
    s.latest_rank,
    CASE WHEN status RLIKE '^(EXPIRED|ACTIVE)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS status_zone,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM deduped s
