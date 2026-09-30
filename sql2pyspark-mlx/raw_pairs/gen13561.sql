SELECT
    dim.city,
    dim.country,
    src.status,
    COUNT(*) AS num_rows,
    MIN(src.balance) AS min_balance
FROM accounts src
INNER JOIN customers dim ON src.customer_id = dim.id
GROUP BY dim.city, dim.country, src.status
ORDER BY country;
