WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.account_id IN (17, 37)
        )
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_amount
    FROM kept s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT account_type
    FROM latest_match
    EXCEPT ALL
    SELECT account_type
    FROM accounts
    WHERE customer_id IN (2, 18)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    JOIN matched_keys k ON s.account_type = k.account_type
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC NULLS FIRST) AS balance_percent_rank,
        SUM(balance) OVER (PARTITION BY customer_id, account_type) AS customer_id_sum_balance
    FROM kept_rows
)
-- Aggregate with conditional metrics
SELECT
    status,
    MAX(balance_percent_rank) FILTER (WHERE status = 'CLOSED') AS closed_max_balance_percent_rank,
    ROUND(AVG(customer_id_sum_balance), 2) AS avg_customer_id_sum_balance,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
FROM scored
GROUP BY ROLLUP (status)
HAVING ROUND(AVG(customer_id_sum_balance), 2) > 0
ORDER BY status
