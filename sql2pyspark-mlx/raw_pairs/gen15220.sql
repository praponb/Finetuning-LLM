WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY DATE_TRUNC('week', txn_time), account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        SUM(amount) OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_2
    FROM transactions
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        a.status,
        a.balance,
        a.opened_date
    FROM with_windows src
    LEFT JOIN accounts a
        ON src.account_id = a.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT txn_type
    FROM enriched
    EXCEPT DISTINCT
    SELECT txn_type
    FROM transactions
    WHERE amount >= 2200
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.txn_type = k.txn_type
),
enriched_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        merchant,
        running_amount,
        balance,
        COUNT(balance) OVER w_grp AS balance_count_90d,
        SUM(running_amount_2) OVER (PARTITION BY YEAR(txn_time), status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS running_amount_2_sum_90d
    FROM kept_rows
    WINDOW w_grp AS (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    txn_type,
    running_amount,
    balance,
    balance_count_90d,
    running_amount_2_sum_90d
FROM enriched_transactions
ORDER BY running_amount DESC NULLS FIRST;
