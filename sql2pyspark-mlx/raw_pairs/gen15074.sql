WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS LAST, id) AS rn
        FROM transactions
    ) x
    WHERE rn = 1
)
SELECT
    id,
    txn_type,
    amount,
    txn_time,
    merchant,
    CASE WHEN merchant RLIKE '^(Starbucks|Amazon)$' THEN 'OTHER_ZONE' WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS merchant_zone
FROM latest;
