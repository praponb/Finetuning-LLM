SELECT
    account_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY account_id) AS cumulative_amount
FROM transactions
WHERE is_flagged
GROUP BY account_id
ORDER BY account_id, total_amount DESC;
