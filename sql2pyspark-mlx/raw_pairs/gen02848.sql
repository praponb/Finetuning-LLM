WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(account_type)) AS account_type_title
    FROM accounts
    WHERE opened_date >= DATE '2023-04-27'
      AND status IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        balance
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY opened_date DESC NULLS LAST, id ASC) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (16, 17, 23, 24)
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        MAX(balance) OVER w_hist AS last7_max_balance,
        AVG(balance) OVER w_hist AS last7_avg_balance,
        COUNT(balance) OVER w_hist AS last7_count_balance,
        SUM(balance) OVER w_hist AS last7_sum_balance
    FROM not_flagged
    WHERE balance > 41000
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY balance ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT id, last7_sum_balance
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last7_max_balance DESC, id ASC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM ranked
    EXCEPT
    SELECT id
    FROM accounts
    WHERE status IN ('CLOSED', 'FROZEN', 'OPEN')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN matched_keys k ON s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND HOUR(c.txn_time) BETWEEN 7 AND 14
        )
)
SELECT id, last7_sum_balance
FROM kept
ORDER BY last7_sum_balance DESC NULLS FIRST;
