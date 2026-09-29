SELECT
    id,
    txn_type,
    CONCAT_WS(',', txn_type, merchant) txn_type_merchant_key,
    LOWER(merchant) merchant_lower
FROM transactions
ORDER BY txn_type_merchant_key;
