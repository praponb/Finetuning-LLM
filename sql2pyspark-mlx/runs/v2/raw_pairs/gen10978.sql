WITH account_summary AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) AS amazon_amount, COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.opened_date,
    a.balance,
    as.amazon_amount,
    as.7_eleven_count
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id;
