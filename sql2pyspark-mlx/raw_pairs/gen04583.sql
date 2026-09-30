WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(balance) OVER w_part AS running_avg_balance,
        MAX(balance) OVER w_part AS running_max_balance,
        COUNT(balance) OVER w_part AS running_count_balance,
        MAX(balance) OVER (PARTITION BY account_type) AS account_type_max_balance
    FROM accounts
    WINDOW w_part AS (PARTITION BY account_type, status ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(running_count_balance) OVER w_part AS next_running_count_balance,
        LAG(balance) OVER w_part AS prev_balance,
        ROW_NUMBER() OVER w_part AS event_seq,
        NTILE(5) OVER (ORDER BY account_type_max_balance DESC, id ASC) AS account_type_max_balance_bucket
    FROM metrics
    WINDOW w_part AS (PARTITION BY account_type, customer_id ORDER BY opened_date ASC, id ASC)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < '2023-11-23'
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM eligible
    EXCEPT ALL
    SELECT customer_id, status
    FROM accounts
    WHERE balance <= 45000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.status = k.status
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        running_avg_balance
    FROM verified
) src
PIVOT (
    AVG(running_avg_balance)
    FOR status IN ('FROZEN' AS frozen, 'OPEN' AS open, 'CLOSED' AS closed)
)
