WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        COUNT(balance) OVER (PARTITION BY status, customer_id) AS status_count_balance,
        COUNT(balance) OVER (PARTITION BY status, account_type) AS status_count_balance_2
    FROM accounts
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.transaction_txn_time,
        x.transaction_amount
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance NULLS LAST, id) AS rn
    FROM with_last
    QUALIFY rn = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN transaction_txn_time - LAG(transaction_txn_time) OVER (PARTITION BY account_type ORDER BY transaction_txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_type ORDER BY transaction_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.age,
        d.segment,
        d.credit_limit
    FROM islands s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.country,
        b.age AS customer_age
    FROM joined e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        account_type,
        transaction_amount
    FROM with_dims
) src
PIVOT (
    SUM(transaction_amount)
    FOR account_type IN ('LOAN', 'SAVINGS')
);
