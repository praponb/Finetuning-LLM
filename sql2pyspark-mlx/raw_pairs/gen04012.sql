SELECT account_id, fee_count, avg_amount
FROM (
    SELECT account_id, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    GROUP BY account_id
) AS s
WHERE fee_count > 6
ORDER BY account_id DESC
LIMIT 20;
