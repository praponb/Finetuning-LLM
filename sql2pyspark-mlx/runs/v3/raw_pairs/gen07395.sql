WITH Base AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (1, 31)
),
agg AS (
    SELECT is_flagged, MAX(amount) AS max_amount, ROUND((100 * SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fee
    FROM Base
    GROUP BY is_flagged
)
SELECT is_flagged, max_amount, pct_fee
FROM agg
WHERE pct_fee > 63
ORDER BY 3 DESC, 2 DESC
