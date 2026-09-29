WITH account_stats AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) credit_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.customer_id, as.credit_count
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id;
