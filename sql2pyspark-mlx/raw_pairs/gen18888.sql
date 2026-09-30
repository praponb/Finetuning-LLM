WITH with_dims AS (
    SELECT
        e.*,
        a.status,
        a.opened_date
    FROM transactions e
    INNER JOIN accounts a
        ON e.account_id = a.id
    WHERE a.account_type = 'SAVINGS'
),
with_dims_transactions AS (
    SELECT
        src.*,
        a.account_type,
        a.opened_date AS account_opened_date,
        a.balance
    FROM with_dims src
    INNER JOIN accounts a
        ON src.account_id = a.id
),
gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_dims_transactions
),
sessions_transactions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_dims_transactions_2 AS (
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.opened_date,
        s.account_type,
        s.account_opened_date,
        s.balance,
        s.session_id,
        a.status AS account_status,
        a.balance AS account_balance
    FROM sessions_transactions s
    INNER JOIN accounts a
        ON s.account_id = a.id
)
SELECT
    CASE WHEN balance >= 87000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    SIZE(COLLECT_SET(account_status)) AS num_account_status,
    COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count,
    SUM(CASE WHEN txn_type = 'FEE' THEN balance ELSE 0 END) AS fee_balance
FROM with_dims_transactions_2
GROUP BY 1
ORDER BY balance_tier NULLS LAST;
