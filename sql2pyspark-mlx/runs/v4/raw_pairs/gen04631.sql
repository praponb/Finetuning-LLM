SELECT id, amount, COALESCE(merchant, 'Unknown') AS merchant_filled
FROM transactions
WHERE TO_DATE(txn_time) < '2025-06-01';
