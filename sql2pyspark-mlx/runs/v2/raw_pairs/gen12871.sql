SELECT
    id,
    merchant,
    amount,
    AVG(amount) OVER (PARTITION BY merchant) AS merchant_avg_amount
FROM transactions
ORDER BY merchant_avg_amount DESC, amount DESC
