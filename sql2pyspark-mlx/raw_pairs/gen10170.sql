WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        INITCAP(TRIM(account_type)) AS account_type_title,
        CONCAT_WS(',', account_type, status) AS account_type_status_key,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-07-26')
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type IN ('FEE', 'CREDIT', 'DEBIT')
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(balance) OVER (ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        DENSE_RANK() OVER (ORDER BY balance) AS balance_dense_rank,
        LAG(balance, 2) OVER (PARTITION BY account_type ORDER BY balance, id) AS prev_balance
    FROM eligible
),
scored_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        status_clean,
        balance_avg_6_prior,
        balance_dense_rank,
        prev_balance,
        COUNT(prev_balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_balance,
        MAX(prev_balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_balance,
        MIN(prev_balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_balance,
        AVG(prev_balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_prev_balance
    FROM scored
    WHERE account_type IN ('CHECKING', 'LOAN')
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance_dense_rank,
    s.running_count_prev_balance,
    s.running_max_prev_balance,
    s.running_min_prev_balance,
    s.running_avg_prev_balance,
    CASE WHEN account_type RLIKE '^(LOAN|CREDIT)$' THEN 'CORE' WHEN account_type RLIKE '^(SAVINGS|CHECKING)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS account_type_zone,
    (SELECT MAX(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS max_account_balance
FROM scored_accounts s;
