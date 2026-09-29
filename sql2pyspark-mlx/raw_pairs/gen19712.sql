WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-04-17 07:00:00'
      AND merchant IS NOT DISTINCT FROM 'Shell'
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.amount,
        src.txn_time,
        src.merchant,
        src.is_flagged_flag,
        src.is_flagged_label,
        p.status,
        p.opened_date
    FROM base src
    LEFT JOIN accounts p
        ON src.account_id = p.id
    WHERE p.customer_id IN (3, 15, 38)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY is_flagged_flag DESC, id) AS rank_in_group
        FROM joined
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(is_flagged_flag) OVER w_running AS running_sum_is_flagged_flag,
        COUNT(is_flagged_flag) OVER w_running AS running_count_is_flagged_flag,
        MAX(is_flagged_flag) OVER w_running AS running_max_is_flagged_flag,
        AVG(is_flagged_flag) OVER w_running AS running_avg_is_flagged_flag
    FROM deduped
    WINDOW w_running AS (PARTITION BY account_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_count_is_flagged_flag >= 16 THEN 'HIGH' ELSE 'LOW' END AS running_count_is_flagged_flag_tier,
    MIN(txn_time) AS started_at,
    PERCENTILE_APPROX(running_max_is_flagged_flag, 0.95) AS running_max_is_flagged_flag_p95,
    COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
    MIN(txn_time) AS started_at_2,
    MAX(txn_time) AS last_seen
FROM enriched
GROUP BY 1
