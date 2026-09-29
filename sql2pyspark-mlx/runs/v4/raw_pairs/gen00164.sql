WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, HOUR(txn_time) AS txn_time_hour
    FROM transactions
    WHERE account_id IN (13, 17)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p2.account_type,
        p2.status
    FROM cleaned a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time_hour DESC, id) AS row_num
        FROM with_dims
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(txn_time_hour) OVER w_ordered AS merchant_min_txn_time_hour,
        SUM(txn_time_hour) OVER (PARTITION BY merchant ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_txn_time_hour,
        LEAD(txn_time_hour, 1) OVER (PARTITION BY account_id ORDER BY amount, id) AS next_txn_time_hour
    FROM latest
    WINDOW w_ordered AS (PARTITION BY merchant, txn_type)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        merchant,
        status,
        account_type,
        amount
    FROM with_windows
) src
PIVOT (
    AVG(amount) AS amount,
    COUNT(1) AS n
    FOR account_type IN ('CHECKING' AS checking, 'CREDIT' AS credit, 'LOAN' AS loan)
)
