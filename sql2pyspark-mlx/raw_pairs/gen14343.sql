WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date), status ORDER BY balance DESC) AS balance_dense_rank,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS FIRST) AS balance_dense_rank_2
    FROM accounts
    WHERE status <> 'FROZEN'
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.balance_dense_rank,
        s.cumulative_balance,
        s.first_known_balance,
        s.balance_dense_rank_2,
        b.city,
        b.age
    FROM enriched s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
),
enriched_accounts_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        balance_dense_rank,
        first_known_balance,
        balance_dense_rank_2,
        age,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY first_known_balance DESC, id) AS first_known_balance_bucket,
        ROUND(age / SUM(age) OVER (PARTITION BY account_type), 4) AS age_share,
        AVG(balance) OVER (ORDER BY balance_dense_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior
    FROM enriched_accounts
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_txn_type
    FROM enriched_accounts_2 s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        first_known_balance,
        balance_dense_rank_2,
        age,
        first_known_balance_bucket,
        balance_avg_6_prior,
        transaction_txn_type,
        NTILE(10) OVER (PARTITION BY transaction_txn_type, account_type ORDER BY balance_dense_rank_2, id) AS balance_dense_rank_2_bucket,
        NTILE(10) OVER (PARTITION BY transaction_txn_type ORDER BY first_known_balance_bucket DESC, id) AS first_known_balance_bucket_bucket,
        AVG(balance_dense_rank) OVER (PARTITION BY transaction_txn_type ORDER BY balance_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_dense_rank_avg_3
    FROM latest_match
)
-- Grouped metrics
SELECT
    account_type,
    COUNT(*) FILTER (WHERE account_type = 'CREDIT') AS credit_rows,
    ROUND(SUM(balance_dense_rank_avg_3) / NULLIF(COUNT(CASE WHEN transaction_txn_type = 'FEE' THEN 1 END), 0), 2) AS balance_dense_rank_avg_3_per_fee,
    PERCENTILE_APPROX(balance_dense_rank_2, 0.95) AS balance_dense_rank_2_p95
FROM scored
GROUP BY account_type
ORDER BY balance_dense_rank_avg_3_per_fee;
