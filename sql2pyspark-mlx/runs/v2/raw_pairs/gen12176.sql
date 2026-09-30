WITH account_totals AS (
    SELECT account_id, COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count, AVG(amount) AS avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    a.balance,
    at.7_eleven_count,
    at.avg_amount
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id;
