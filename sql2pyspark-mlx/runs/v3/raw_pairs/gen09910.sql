SELECT
    txn_type,
    merchant,
    amount,
    ROUND(amount * 0.9, 2) AS amount_adjusted
FROM transactions
WHERE account_id IN (10, 31, 33) AND merchant = 'Shell' AND amount > 3800;
