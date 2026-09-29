WITH filtered AS (
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2023-08-20 07:00:00')
)
SELECT *
FROM (
    SELECT
        merchant,
        txn_type,
        amount
    FROM filtered
) src
PIVOT (
    AVG(amount)
    FOR txn_type IN ('CREDIT', 'REFUND')
)
