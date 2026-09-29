SELECT
    amount,
    account_id,
    CONCAT_WS('-', merchant, txn_type) merchant_txn_type_key,
    ROUND(amount * 1.07, 2) amount_adjusted
FROM transactions
WHERE account_id IN (5, 28);
