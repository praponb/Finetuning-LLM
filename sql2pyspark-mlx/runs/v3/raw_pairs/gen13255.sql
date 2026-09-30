SELECT merchant, MAX(amount) - MIN(amount) amount_range
FROM transactions
WHERE txn_type IN ('DEBIT', 'CREDIT') AND txn_type = 'FEE' AND HOUR(txn_time) BETWEEN 10 AND 16
GROUP BY 1
ORDER BY amount_range
