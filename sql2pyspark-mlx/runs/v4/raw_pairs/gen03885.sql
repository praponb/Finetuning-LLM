WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id, account_type ORDER BY balance NULLS LAST) AS latest_rank
        FROM accounts
    ) t
    WHERE latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(status, TRUE) OVER (ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_status,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) seq_no,
        AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) balance_avg_3_prior
    FROM ranked
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.txn_time,
        x.amount
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.txn_time, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.txn_time
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS amount_per_open,
    SUM(CASE WHEN account_type = 'CREDIT' THEN seq_no ELSE 0 END) AS credit_seq_no,
    MAX(txn_time) AS last_txn_time
FROM kept
GROUP BY DATE_TRUNC('WEEK', opened_date)
HAVING SUM(CASE WHEN account_type = 'CREDIT' THEN seq_no ELSE 0 END) >= 0
ORDER BY week_start DESC NULLS LAST;
