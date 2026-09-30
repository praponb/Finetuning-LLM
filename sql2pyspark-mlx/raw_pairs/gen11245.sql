WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY MONTH(opened_date), account_type ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS balance_avg_2,
        SUM(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        LEAD(balance, 1, 0) OVER (PARTITION BY customer_id ORDER BY balance, id) AS next_balance
    FROM accounts
    WHERE MONTH(opened_date) IN (1, 3, 11)
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.account_type,
        a.balance,
        a.balance_avg_2,
        d.signup_date,
        d.city,
        d.name
    FROM metrics a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
metrics_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        name,
        MAX(balance) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance,
        MIN(balance) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_balance,
        AVG(balance) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance
    FROM with_dims
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics_accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.amount IS NOT NULL
        )
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.name,
        s.running_max_balance,
        s.running_avg_balance,
        top1.transaction_amount
    FROM eligible s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_type = 'DEBIT'
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_max_balance >= 58000 THEN 'HIGH' ELSE 'LOW' END AS running_max_balance_tier,
    COUNT(DISTINCT account_type) AS distinct_account_types,
    COUNT_IF(account_type = 'LOAN') AS loan_events,
    SIZE(COLLECT_SET(account_type)) AS num_account_types
FROM not_flagged
GROUP BY running_max_balance_tier
