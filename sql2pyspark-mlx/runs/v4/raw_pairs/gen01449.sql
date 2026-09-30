SELECT account_id, DATE_TRUNC('MONTH', txn_time) AS txn_month, MAX(amount) AS max_amount
FROM transactions
WHERE (merchant = 'Shell' AND txn_time < '2024-08-10') OR merchant IS NOT NULL
GROUP BY account_id, DATE_TRUNC('MONTH', txn_time)
