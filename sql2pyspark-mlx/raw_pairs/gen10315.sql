WITH with_latest AS (
    SELECT
        s.id,
        s.account_type,
        s.balance,
        top1.transaction_txn_type
    FROM accounts s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    SELECT
        id,
        balance,
        AVG(balance) OVER (ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS balance_avg_3_prior,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) AS seq_no
    FROM with_latest
    WHERE transaction_txn_type = 'DEBIT'
),
kept AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (6, 15, 31, 34)
        )
),
latest_match AS (
    SELECT
        s.*,
        top1.txn_time,
        top1.transaction_amount
    FROM kept s
    JOIN LATERAL (
            SELECT c.txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept_accounts AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.txn_time
        )
)
SELECT
    CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    CASE WHEN balance_avg_2_prior >= 77076 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_avg_2_prior_tier,
    MAX(transaction_amount) AS max_transaction_amount,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    APPROX_PERCENTILE(balance, 0.5) AS balance_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(txn_time) AS last_seen
FROM kept_accounts
GROUP BY CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN balance_avg_2_prior >= 77076 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY distinct_ids DESC
