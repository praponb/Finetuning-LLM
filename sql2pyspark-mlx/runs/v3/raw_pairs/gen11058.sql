SELECT
    segment,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(credit_limit) DESC) AS credit_limit_rank
FROM customers
GROUP BY segment
ORDER BY credit_limit_rank DESC;
