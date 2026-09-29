WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN amount >= 4300 THEN 'High' WHEN amount >= 600 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE merchant IN ('Shell', 'Grab', '7-Eleven')
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC NULLS LAST, id) = 1;
