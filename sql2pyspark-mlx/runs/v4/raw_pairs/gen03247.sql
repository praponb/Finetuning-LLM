WITH filtered AS (
    SELECT *, CASE WHEN balance >= 65000 THEN 'High' WHEN balance >= 20000 THEN 'Medium' ELSE 'Low' END AS balance_tier
    FROM accounts
    WHERE opened_date >= '2025-01-26'
),
filtered_accounts AS (
    SELECT
        id,
        balance,
        opened_date,
        balance_tier,
        CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END AS is_open,
        COALESCE(balance, 0) AS balance_filled,
        COALESCE(status, 'UNKNOWN') AS resolved_status
    FROM filtered
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        LEAD(is_open, 1) OVER (ORDER BY is_open, id) AS next_is_open,
        ROW_NUMBER() OVER w_hist AS row_seq,
        NTILE(5) OVER w_hist AS balance_filled_bucket
    FROM filtered_accounts
    WINDOW w_hist AS (ORDER BY balance_filled DESC, id)
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(row_seq) IGNORE NULLS OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        AVG(is_open) OVER (ORDER BY next_is_open, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS is_open_avg_3
    FROM with_windows
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
latest_match AS (
    SELECT s.*, top1.txn_type
    FROM eligible s
    JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
)
SELECT *
FROM (
    SELECT
        id,
        txn_type,
        balance_filled,
        balance
    FROM not_flagged
) src
UNPIVOT (
    val FOR kpi IN (balance_filled, balance)
);
