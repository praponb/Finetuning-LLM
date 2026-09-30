WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE is_flagged = TRUE AND amount <= 2000 AND NOT is_flagged
),
agg AS (
    SELECT merchant, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
    FROM valid_transactions
    GROUP BY merchant
)
SELECT *
FROM agg;
