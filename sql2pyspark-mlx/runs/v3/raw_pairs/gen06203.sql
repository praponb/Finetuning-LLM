SELECT
    is_flagged,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM transactions
WHERE amount > 1200
GROUP BY is_flagged
ORDER BY rnk, cnt, is_flagged
LIMIT 1;
