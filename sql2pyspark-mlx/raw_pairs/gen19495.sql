SELECT DATE_TRUNC('MONTH', txn_time) AS txn_month, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
WHERE txn_type NOT IN ('REFUND', 'CREDIT', 'DEBIT') AND account_id = 19 AND amount > 3300
GROUP BY DATE_TRUNC('MONTH', txn_time)
