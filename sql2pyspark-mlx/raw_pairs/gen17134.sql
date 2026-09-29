WITH joined AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.amount,
        s.merchant,
        s.is_flagged,
        b.account_type,
        b.status,
        b.balance
    FROM transactions s
    LEFT JOIN accounts b
        ON s.account_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY merchant, account_type ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
        AVG(amount) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        STDDEV_SAMP(balance) OVER (PARTITION BY merchant) AS merchant_stddev_samp_balance,
        NTILE(5) OVER (PARTITION BY account_type ORDER BY amount DESC, id) AS amount_bucket,
        AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS amount_avg_6
    FROM joined
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        a.opened_date,
        a.account_type AS account_account_type,
        a.status AS account_status
    FROM with_windows e
    LEFT JOIN accounts a
        ON e.account_id = a.id
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p2.balance AS account_balance,
        p2.account_type AS account_account_type_2,
        p2.opened_date AS account_opened_date
    FROM enriched a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
    WHERE p2.opened_date BETWEEN '2025-10-01' AND '2025-10-26'
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        is_flagged,
        account_type,
        status,
        amount_avg_2_prior,
        amount_avg_5,
        opened_date,
        account_balance,
        account_opened_date,
        ROW_NUMBER() OVER (PARTITION BY account_account_type ORDER BY account_opened_date, id) AS seq_no,
        AVG(balance) OVER (PARTITION BY status, account_account_type) AS status_avg_balance,
        ROW_NUMBER() OVER (PARTITION BY account_status ORDER BY balance, id) AS event_seq,
        AVG(merchant_stddev_samp_balance) OVER (ORDER BY merchant_stddev_samp_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS merchant_stddev_samp_balance_avg_3,
        NTILE(4) OVER (PARTITION BY account_id ORDER BY amount_avg_6, id) AS amount_avg_6_bucket
    FROM joined_transactions
)
SELECT
    s.id,
    s.account_id,
    s.is_flagged,
    s.status,
    s.amount_avg_2_prior,
    s.amount_avg_5,
    s.opened_date,
    s.account_balance,
    s.seq_no,
    (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS avg_transaction_amount
FROM with_windows_transactions s
