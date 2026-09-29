SELECT
    d.country,
    d.segment,
    COUNT(*) AS num_rows,
    MIN(s.balance) AS min_balance
FROM accounts s
INNER JOIN customers d ON s.customer_id = d.id
GROUP BY GROUPING SETS ((d.country, d.segment), (d.country));
