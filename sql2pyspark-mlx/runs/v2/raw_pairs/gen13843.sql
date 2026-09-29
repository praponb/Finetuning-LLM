WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE amount > 3000
),
TransactionsStats AS (
    SELECT HOUR(txn_time) AS txn_hour, is_flagged, MAX(amount) - MIN(amount) AS amount_range
    FROM valid_transactions
    GROUP BY HOUR(txn_time), is_flagged
)
SELECT *
FROM TransactionsStats
ORDER BY amount_range DESC;
