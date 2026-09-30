WITH account_totals AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount, COUNT(*) AS num_transactions
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    a.account_type,
    at.credit_amount,
    at.num_transactions
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
WHERE at.num_transactions >= 4;
