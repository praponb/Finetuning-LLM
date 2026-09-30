SELECT id, amount, merchant FROM transactions WHERE txn_type IN ('CREDIT', 'FEE') OR txn_time >= '2024-10-31' ORDER BY 2 DESC;
