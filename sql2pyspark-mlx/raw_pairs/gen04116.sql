SELECT id, amount, merchant FROM transactions WHERE is_flagged = TRUE AND (txn_time >= '2023-11-18' OR TO_DATE(txn_time) >= '2025-06-25');
