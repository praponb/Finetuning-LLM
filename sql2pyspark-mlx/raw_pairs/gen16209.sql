WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
        SUM(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
    FROM accounts
)
SELECT
    id,
    balance,
    opened_date,
    status,
    last_known_status,
    cumulative_balance,
    CASE WHEN status RLIKE '^(FROZEN|CLOSED)$' THEN 'EXTENDED' WHEN status RLIKE '^(OPEN)$' THEN 'GROUP_B' ELSE 'CORE' END AS status_zone
FROM enriched
ORDER BY status NULLS FIRST, balance;
