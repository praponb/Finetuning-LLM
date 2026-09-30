SELECT *
FROM (
    SELECT txn_type, is_flagged, MIN(amount) min_amount
    FROM transactions
    GROUP BY txn_type, is_flagged
) sub
WHERE min_amount > 1800
ORDER BY min_amount
