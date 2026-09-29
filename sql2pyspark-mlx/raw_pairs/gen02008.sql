WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(balance, 0) AS balance_filled
    FROM accounts
    WHERE status IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY balance NULLS FIRST, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_filled,
        CONCAT(account_type, ' - ', status) AS account_type_status
    FROM latest
    WHERE status IS NOT NULL
      AND status IS NOT DISTINCT FROM 'OPEN'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        SUM(balance) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        SUM(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance_2,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior
    FROM not_flagged
    WHERE status = 'OPEN'
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    cumulative_balance_2
FROM scored
ORDER BY id
LIMIT 20;
