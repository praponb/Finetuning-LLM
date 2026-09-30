WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        status,
        TRIM(account_type) AS clean_account_type,
        INITCAP(TRIM(account_type)) AS account_type_title
    FROM accounts
    WHERE opened_date > TO_DATE('2023-05-20')
      AND status IS NOT NULL
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.merchant,
        latest.amount
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.merchant, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_amount
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY merchant, status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY transaction_amount DESC, id) AS row_seq
    FROM with_latest
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY amount, id) AS row_seq_2,
        SUM(amount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        LEAD(amount, 1, 0) OVER (PARTITION BY status, customer_id ORDER BY amount, id) AS next_amount
    FROM enriched
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_amount DESC, id) AS rank_in_group
        FROM with_windows
    ) x
    WHERE rank_in_group = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        merchant,
        row_seq
    FROM latest
) src
PIVOT (
    SUM(row_seq)
    FOR merchant IN ('Shell', 'Lazada')
)
