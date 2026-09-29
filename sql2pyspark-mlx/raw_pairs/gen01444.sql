SELECT id, is_flagged, DATE_TRUNC('year', txn_time) AS txn_time_period
FROM transactions
WHERE amount < 1800 AND txn_type NOT IN ('DEBIT', 'FEE', 'REFUND')
