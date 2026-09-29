WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        LENGTH(merchant) AS merchant_length
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-06-30 08:00:00')
)
-- Grouped metrics
SELECT
    CASE WHEN merchant_length >= 66 THEN 'HIGH' ELSE 'LOW' END AS merchant_length_tier,
    merchant,
    COUNT(*) FILTER (WHERE txn_type = 'REFUND') AS refund_rows,
    COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count,
    ROUND(AVG(merchant_length), 2) AS avg_merchant_length
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN merchant_length >= 66 THEN 'HIGH' ELSE 'LOW' END, merchant), (CASE WHEN merchant_length >= 66 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING refund_rows > 1
ORDER BY refund_rows NULLS FIRST
