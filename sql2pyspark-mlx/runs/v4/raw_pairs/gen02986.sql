WITH kept_keys AS (
    SELECT customer_id
    FROM accounts
    INTERSECT ALL
    SELECT customer_id
    FROM accounts
    WHERE opened_date BETWEEN '2024-11-13' AND '2025-01-06'
),
kept_rows AS (
    SELECT s.*
    FROM accounts s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
not_flagged AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status, account_type ORDER BY balance NULLS FIRST) AS balance_dense_rank,
        RANK() OVER (PARTITION BY account_type, customer_id ORDER BY balance) AS balance_rank,
        MAX(balance) OVER (PARTITION BY customer_id) AS customer_id_max_balance,
        STDDEV_SAMP(balance) OVER (PARTITION BY account_type) AS account_type_stddev_samp_balance,
        PERCENT_RANK() OVER w_ordered AS balance_percent_rank
    FROM not_flagged
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY balance NULLS FIRST)
)
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    ROUND(SUM(balance_rank) / NULLIF(COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END), 0), 2) AS balance_rank_per_checking,
    COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END) AS credit_count,
    ROUND(AVG(balance_percent_rank), 2) AS avg_balance_percent_rank,
    AVG(balance_dense_rank) FILTER (WHERE account_type = 'SAVINGS') AS savings_avg_balance_dense_rank,
    ROUND(AVG(customer_id_max_balance), 2) AS avg_customer_id_max_balance
FROM metrics
GROUP BY DATE_TRUNC('WEEK', opened_date)
