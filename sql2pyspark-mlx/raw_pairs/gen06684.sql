SELECT
    id,
    txn_time,
    CAST(amount AS INT) AS amount_int,
    UPPER(merchant) AS merchant_upper
FROM transactions
WHERE is_flagged
ORDER BY amount_int DESC;
