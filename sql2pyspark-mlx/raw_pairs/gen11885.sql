WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM transactions
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        running_amount,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM scored
    WHERE txn_time >= TO_DATE('2023-09-01')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        AVG(is_flagged_flag) OVER (PARTITION BY txn_type, account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_flagged_flag_avg_7d
    FROM cleaned
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.status,
        p.balance,
        p.account_type
    FROM enriched e
    INNER JOIN accounts p
        ON e.account_id = p.id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT s.*, p.opened_date
    FROM enriched_transactions s
    LEFT JOIN accounts p
        ON s.account_id = p.id
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT a.*, p2.status AS account_status
    FROM with_dims a
    INNER JOIN accounts p2
        ON a.account_id = p2.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        merchant,
        is_flagged,
        is_flagged_flag,
        last_known_merchant,
        status,
        balance,
        LAG(balance, 1, 0) OVER w_recent AS prev_balance,
        LAG(is_flagged_flag_avg_7d, 1) OVER w_ordered AS prev_is_flagged_flag_avg_7d,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(txn_time), status ORDER BY is_flagged_flag, id) AS seq_no,
        AVG(running_amount) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_amount_avg_5_prior,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant
    FROM with_dims_transactions
    WINDOW w_recent AS (ORDER BY is_flagged_flag, id), w_ordered AS (ORDER BY balance, id)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        merchant,
        amount
    FROM with_windows
) src
PIVOT (
    MAX(amount) AS total,
    COUNT(1) AS item_count
    FOR merchant IN ('Shell', 'Grab', 'Starbucks')
)
