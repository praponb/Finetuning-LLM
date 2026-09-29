WITH cleaned AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        SUBSTRING(status, 1, 4) AS status_prefix,
        CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit
    FROM accounts
    WHERE status IS NOT NULL
      AND account_type = 'CHECKING'
),
base AS (
    SELECT
        id,
        customer_id,
        balance,
        status,
        status_prefix,
        is_credit,
        SUBSTRING(status, 1, 2) AS status_prefix_2
    FROM cleaned
    WHERE account_type IN ('LOAN', 'CHECKING', 'CREDIT')
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged
        )
)
SELECT
    status,
    PERCENTILE_APPROX(is_credit, 0.95) AS is_credit_p95,
    COUNT_IF(status = 'FROZEN') AS frozen_events,
    PERCENTILE_APPROX(balance, 0.9) AS balance_p90
FROM not_flagged
GROUP BY status;
