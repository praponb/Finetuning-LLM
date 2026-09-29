WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        SUBSTRING(merchant, 1, 3) AS merchant_prefix
    FROM transactions
    WHERE merchant IN ('Starbucks', 'Lazada')
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 4000 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    AVG(amount) AS avg_amount,
    ROUND(AVG(amount), 2) AS avg_amount_2,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned
GROUP BY 1;
