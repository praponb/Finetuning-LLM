WITH ranked AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time NULLS FIRST, id) = 1
),
latest AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) = 1
),
kept_keys AS (
    SELECT account_id
    FROM latest
    EXCEPT
    SELECT account_id
    FROM transactions
    WHERE txn_time < '2024-07-01'
),
verified AS (
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.account_id = k.account_id
),
cleaned AS (
    SELECT *, COALESCE(merchant, 'N/A') AS merchant_filled
    FROM verified
    WHERE txn_time >= TIMESTAMP '2023-09-29 22:00:00'
      AND merchant IS NOT NULL
),
valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        merchant_filled,
        ROUND(amount * 100, 2) AS amount_adjusted,
        UPPER(txn_type) AS txn_type_upper
    FROM cleaned
    WHERE txn_time > TIMESTAMP '2023-06-10 01:00:00'
      AND is_flagged
)
SELECT *
FROM (
    SELECT
        account_id,
        merchant,
        amount_adjusted
    FROM valid
) src
PIVOT (
    MAX(amount_adjusted) AS value_sum,
    COUNT(1) AS n
    FOR merchant IN ('7-Eleven' AS v_7_eleven, 'Lazada' AS lazada, 'Starbucks' AS starbucks)
);
