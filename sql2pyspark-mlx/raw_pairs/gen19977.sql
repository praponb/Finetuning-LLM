WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        CASE WHEN txn_type IN ('DEBIT', 'FEE') THEN 'Core' ELSE 'Other' END AS txn_type_group
    FROM transactions
    WHERE merchant IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        LAG(amount, 1) OVER w_recent AS prev_amount,
        SUM(amount) OVER (PARTITION BY TO_DATE(txn_time), txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        AVG(amount) OVER w_part AS amount_avg_7d,
        MAX(amount) OVER w_part AS amount_max_7d
    FROM valid
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY amount, id), w_part AS (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        amount,
        txn_type_group,
        first_known_amount,
        prev_amount,
        cumulative_amount,
        amount_max_7d,
        LEAD(amount) OVER w_part AS next_amount,
        LAG(amount) OVER w_part AS prev_amount_2,
        LAG(prev_amount) OVER w_part AS prev_prev_amount,
        ROW_NUMBER() OVER w_part AS event_seq,
        SUM(prev_amount) OVER (PARTITION BY txn_type ORDER BY cumulative_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_amount
    FROM with_windows
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY cumulative_amount, id)
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.first_known_amount,
        e.cumulative_amount,
        e.amount_max_7d,
        e.prev_prev_amount,
        e.event_seq,
        a.status,
        a.account_type,
        a.opened_date
    FROM with_windows_transactions e
    JOIN accounts a
        ON e.account_id = a.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY first_known_amount DESC NULLS FIRST) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
latest_transactions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, status ORDER BY cumulative_amount, id) AS rank_in_group_2
        FROM latest
    ) x
    WHERE rank_in_group_2 = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY prev_prev_amount DESC, id) AS row_num
        FROM latest_transactions
    ) t
    WHERE row_num = 1
)
-- Running and moving aggregates
SELECT
    *,
    LAG(prev_prev_amount) OVER w_part AS prev_prev_prev_amount,
    LAG(amount_max_7d) OVER w_part AS prev_amount_max_7d,
    LEAD(amount_max_7d) OVER w_part AS next_amount_max_7d,
    ROW_NUMBER() OVER w_part AS seq_no,
    FIRST_VALUE(account_type, TRUE) OVER (PARTITION BY status ORDER BY amount_max_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type
FROM deduped
WINDOW w_part AS (PARTITION BY account_id, status ORDER BY first_known_amount, id);
