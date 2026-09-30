WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        SUBSTRING(status, 1, 2) AS status_prefix,
        DATE_SUB(opened_date, 44) AS opened_date_minus_44d
    FROM accounts
    WHERE status IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        opened_date_minus_44d,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance,
        MAX(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance,
        COUNT(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_balance,
        LEAD(balance, 2) OVER (PARTITION BY account_type ORDER BY balance, id) AS next_balance
    FROM valid
    WHERE balance < 77000
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_max_balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_max_balance_avg_6_prior,
        SUM(running_count_balance) OVER (PARTITION BY account_type ORDER BY running_count_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_count_balance
    FROM scored
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date_minus_44d
        )
)
SELECT
    id,
    account_type,
    opened_date,
    opened_date_minus_44d,
    running_avg_balance,
    running_max_balance,
    running_count_balance,
    next_balance,
    running_max_balance_avg_6_prior,
    CASE WHEN account_type RLIKE '^(CHECKING|SAVINGS)$' THEN 'GROUP_B' WHEN account_type RLIKE '^(CREDIT|LOAN)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS account_type_zone
FROM not_flagged
ORDER BY account_type;
