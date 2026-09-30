WITH AccountTotals AS (
    SELECT account_id, COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count, COUNT(*) AS row_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.balance, A.7_eleven_count, A.row_count
FROM accounts a
LEFT JOIN AccountTotals A ON a.id = A.account_id
WHERE A.row_count >= 6;
