WITH with_dims AS (
    -- Attach accounts attributes
    SELECT src.*, a.status
    FROM transactions src
    INNER JOIN accounts a
        ON src.account_id = a.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, txn_type
    FROM with_dims
    EXCEPT DISTINCT
    SELECT id, txn_type
    FROM transactions
    WHERE txn_type = 'REFUND'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.txn_type = k.txn_type
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount) OVER w_grp AS amount_count_7d,
        MAX(amount) OVER w_grp_2 AS amount_max_7d,
        STDDEV_SAMP(amount) OVER w_grp_3 AS status_stddev_samp_amount,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_90d
    FROM kept_rows
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_grp_3 AS (PARTITION BY status)
)
-- Grouped metrics
SELECT
    account_id,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    SUM(CASE WHEN status = 'FROZEN' THEN status_stddev_samp_amount ELSE 0 END) AS frozen_status_stddev_samp_amount,
    COUNT(DISTINCT account_id) AS distinct_account_ids_2,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(txn_time) AS ended_at
FROM enriched
GROUP BY account_id
HAVING num_status >= 1
ORDER BY num_status DESC NULLS LAST;
