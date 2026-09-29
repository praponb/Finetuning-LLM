SELECT *
FROM (
    SELECT is_flagged, MIN(amount) min_amount, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) fee_count
    FROM transactions
    GROUP BY is_flagged
) sub
WHERE fee_count < 9
