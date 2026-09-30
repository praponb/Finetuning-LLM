SELECT txn_type, stddev_amount, fee_amount
FROM (
    SELECT txn_type, ROUND(STDDEV(amount), 2) AS stddev_amount, SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount
    FROM transactions
    WHERE merchant IN ('7-Eleven', 'Starbucks', 'Grab')
    GROUP BY txn_type
) AS sub
WHERE fee_amount >= 68
ORDER BY 1;
