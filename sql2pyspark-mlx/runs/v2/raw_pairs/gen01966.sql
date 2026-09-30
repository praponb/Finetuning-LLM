WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE merchant != 'Grab'
),
transactions_stats AS (
    SELECT is_flagged, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, COUNT(*) AS cnt
    FROM valid_transactions
    GROUP BY is_flagged, DATE_FORMAT(txn_time, 'yyyy-MM')
)
SELECT *
FROM transactions_stats
