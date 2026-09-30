SELECT
    merchant,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY merchant) AS cumulative_amount
FROM transactions
WHERE txn_type <> 'DEBIT'
GROUP BY merchant
ORDER BY cumulative_amount DESC;
