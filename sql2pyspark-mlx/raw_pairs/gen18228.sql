WITH filtered AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE status IS NOT NULL
),
enriched AS (
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_5_prior,
        MIN(balance) OVER (PARTITION BY status) AS status_min_balance,
        SUM(balance) OVER w_recent AS running_balance
    FROM filtered
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY balance_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.opened_date,
        s.balance_avg_5_prior,
        top1.txn_time,
        top1.txn_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.txn_time, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined AS (
    SELECT
        s.*,
        b.signup_date,
        b.credit_limit,
        b.age
    FROM with_latest s
    JOIN customers b
        ON s.customer_id = b.id
),
with_dims AS (
    SELECT
        s.*,
        d.name,
        d.signup_date AS customer_signup_date
    FROM joined s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.city = 'Singapore'
),
metrics AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY txn_type ORDER BY credit_limit DESC, id) AS credit_limit_bucket,
        LEAD(balance, 2) OVER (PARTITION BY txn_type, customer_id ORDER BY txn_time, id) AS next_balance
    FROM with_dims
)
SELECT
    id,
    customer_id,
    opened_date,
    txn_time,
    txn_type,
    signup_date,
    credit_limit,
    name,
    customer_signup_date
FROM metrics
ORDER BY id DESC, opened_date
