SELECT
    account_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM transactions
WHERE txn_type IN ('FEE', 'REFUND', 'DEBIT')
GROUP BY account_id
ORDER BY pct_of_total DESC;
