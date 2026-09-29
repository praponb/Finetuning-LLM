WITH account_totals AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.opened_date, at.fee_amount
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id
WHERE at.fee_amount >= 31
