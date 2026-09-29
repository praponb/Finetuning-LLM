SELECT MAX(amount) - MIN(amount) AS amount_range, COUNT(DISTINCT amount) AS unique_txn_types FROM transactions;
