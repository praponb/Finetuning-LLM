SELECT
    dim.city,
    s.account_type,
    COUNT(*) AS num_rows,
    MAX(s.balance) AS max_balance
FROM accounts s
INNER JOIN customers dim ON s.customer_id = dim.id
GROUP BY dim.city, s.account_type;
