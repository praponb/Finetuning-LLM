WITH base AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) / 7 AS weeks_since_opened_date,
        CONCAT_WS(',', account_type, status) AS account_type_status_key,
        COALESCE(status, 'OTHER') AS resolved_status
    FROM accounts
    WHERE customer_id IS NULL
),
with_latest AS (
    SELECT
        s.id,
        s.status,
        s.weeks_since_opened_date,
        s.account_type_status_key,
        latest.transaction_txn_time,
        latest.transaction_amount
    FROM base s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
remaining_keys AS (
    SELECT status, id
    FROM with_latest
    EXCEPT ALL
    SELECT status, id
    FROM accounts
    WHERE status = 'CLOSED'
),
kept_rows AS (
    SELECT s.*
    FROM with_latest s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
with_windows AS (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY transaction_amount DESC NULLS FIRST) AS transaction_amount_rank
    FROM kept_rows
),
scored AS (
    SELECT
        weeks_since_opened_date,
        transaction_amount,
        transaction_amount_rank,
        COUNT(transaction_amount_rank) OVER w_running AS transaction_amount_rank_count_90d,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('month', transaction_txn_time), status ORDER BY weeks_since_opened_date DESC) AS weeks_since_opened_date_cume_dist,
        AVG(transaction_amount_rank) OVER w_running AS transaction_amount_rank_avg_90d,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY transaction_amount NULLS FIRST) AS transaction_amount_dense_rank
    FROM with_windows
    WHERE weeks_since_opened_date >= 36
    WINDOW w_running AS (PARTITION BY status ORDER BY CAST(transaction_txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
)
SELECT
    weeks_since_opened_date,
    transaction_amount,
    transaction_amount_rank,
    weeks_since_opened_date_cume_dist,
    transaction_amount_rank_avg_90d,
    RANK() OVER (ORDER BY transaction_amount) AS transaction_amount_rank_2
FROM scored;
