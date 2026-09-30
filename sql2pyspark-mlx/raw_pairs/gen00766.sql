SELECT
    account_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM transactions
WHERE amount < 2000
GROUP BY account_id
ORDER BY account_id ASC
