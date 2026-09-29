WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE txn_type IN ('REFUND', 'FEE', 'DEBIT')
),
is_flagged_summary AS (
    SELECT is_flagged, SUM(amount) AS total_amount, MIN(amount) AS min_amount
    FROM valid_transactions
    GROUP BY is_flagged
)
SELECT is_flagged, total_amount, min_amount
FROM is_flagged_summary;
