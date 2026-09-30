SELECT
    c.country,
    c.segment,
    src.account_type,
    COUNT(*) AS num_rows,
    MAX(src.balance) AS max_balance
FROM accounts src
LEFT JOIN customers c ON src.customer_id = c.id
GROUP BY CUBE (c.country, c.segment, src.account_type)
ORDER BY account_type
