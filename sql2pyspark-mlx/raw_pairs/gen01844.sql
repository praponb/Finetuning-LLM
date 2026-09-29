SELECT id, account_id, merchant FROM transactions WHERE TO_DATE(txn_time) < '2023-12-23' EXCEPT SELECT id, account_id, merchant FROM transactions WHERE TO_DATE(txn_time) >= '2025-03-02' ORDER BY id
