SELECT id, txn_time, merchant, amount, DATE_TRUNC('month', txn_time) AS txn_time_period FROM transactions WHERE account_id = 16 AND is_flagged ORDER BY amount DESC, id ASC LIMIT 10
