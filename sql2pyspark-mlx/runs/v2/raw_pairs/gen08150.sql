WITH ValidTransactions AS (
    SELECT *, DATE_TRUNC('year', txn_time) AS txn_time_period
    FROM transactions
    WHERE merchant = 'Amazon'
),
is_flagged_summary AS (
    SELECT is_flagged, COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count
    FROM ValidTransactions
    GROUP BY is_flagged
)
SELECT is_flagged, credit_count
FROM is_flagged_summary
WHERE credit_count < 8;
