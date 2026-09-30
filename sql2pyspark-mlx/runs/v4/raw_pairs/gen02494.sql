WITH transactions_by_account AS (
    SELECT account_id, AVG(amount) AS avg_amount, SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) AS amazon_amount
    FROM transactions
    WHERE account_id IN (12, 13, 17)
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    tba.avg_amount,
    tba.amazon_amount
FROM accounts a
JOIN transactions_by_account tba ON a.id = tba.account_id
