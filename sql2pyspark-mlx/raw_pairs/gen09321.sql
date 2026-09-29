WITH with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        latest.amount
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY amount, id) AS rank_in_group
        FROM with_latest
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER w_grp AS running_avg_amount,
        MIN(amount) OVER w_grp AS running_min_amount,
        SUM(amount) OVER w_grp AS running_sum_amount,
        MAX(amount) OVER w_grp AS running_max_amount
    FROM deduped
    WINDOW w_grp AS (PARTITION BY account_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant <> 'Lazada'
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= '2024-05-23'
        )
)
-- Window metrics over several different partitions
SELECT
    id,
    rank_in_group,
    running_sum_amount,
    FIRST_VALUE(amount, TRUE) OVER (PARTITION BY account_type ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    ROUND(running_max_amount / SUM(running_max_amount) OVER (PARTITION BY account_type), 4) AS running_max_amount_share,
    SUM(running_sum_amount) OVER (PARTITION BY account_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sum_amount,
    NTILE(5) OVER (PARTITION BY account_type ORDER BY running_max_amount, id) AS running_max_amount_bucket
FROM eligible
WHERE amount >= 1500
