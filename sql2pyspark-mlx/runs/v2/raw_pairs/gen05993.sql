SELECT account_id, total_amount
FROM (
    SELECT account_id, SUM(amount) AS total_amount
    FROM transactions
    WHERE txn_type != 'CREDIT'
    GROUP BY account_id
) AS s
WHERE total_amount >= 19000
ORDER BY total_amount DESC;
