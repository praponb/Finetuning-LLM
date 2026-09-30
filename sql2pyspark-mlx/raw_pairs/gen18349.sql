WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        TRIM(merchant) AS clean_merchant,
        CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-05-08 16:00:00')
      AND account_id IN (11, 20, 22)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        account_id,
        txn_type,
        clean_merchant,
        txn_type_merchant_key
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time, id) = 1
),
filtered_transactions AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(txn_type, 1, 2) AS txn_type_prefix
    FROM latest
    WHERE account_id IN (12, 16)
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.account_type,
        a.balance,
        a.opened_date
    FROM filtered_transactions s
    LEFT JOIN accounts a
        ON s.account_id = a.id
    WHERE YEAR(a.opened_date) >= 2024
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        txn_type,
        account_id,
        account_type,
        balance
    FROM enriched
) src
PIVOT (
    MAX(balance) AS amount,
    COUNT(1) AS item_count
    FOR account_type IN ('LOAN' AS loan, 'CHECKING' AS checking)
)
