WITH transactions_by_account AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 8 AND 12
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.status,
    tba.avg_amount
FROM accounts a
JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.avg_amount >= 79
ORDER BY balance;
