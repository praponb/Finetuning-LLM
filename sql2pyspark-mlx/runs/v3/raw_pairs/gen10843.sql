WITH transactions_by_account AS (
    SELECT account_id, AVG(amount) AS avg_amount, COUNT(*) AS cnt
    FROM transactions
    WHERE amount BETWEEN 2300 AND 3900
    GROUP BY account_id
)
SELECT
    a.account_type,
    a.balance,
    tba.avg_amount,
    tba.cnt
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY balance DESC;
