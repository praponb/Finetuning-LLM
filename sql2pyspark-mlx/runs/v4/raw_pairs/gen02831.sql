WITH base AS (
    SELECT *
    FROM transactions
    WHERE amount < 1500
),
agg AS (
    SELECT is_flagged, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM base
    GROUP BY is_flagged
)
SELECT is_flagged, txn_type_values
FROM agg
ORDER BY txn_type_values DESC, is_flagged
LIMIT 5;
