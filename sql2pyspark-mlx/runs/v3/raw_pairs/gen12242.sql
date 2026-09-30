WITH transactions_by_account AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount
    FROM transactions
    WHERE amount IS NOT NULL
    GROUP BY account_id
)
SELECT a.balance, tba.fee_amount
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id;
