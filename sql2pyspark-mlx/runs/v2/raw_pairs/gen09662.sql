SELECT
    txn_type,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM transactions
GROUP BY txn_type
ORDER BY rnk DESC;
