SELECT
    d.status,
    s.merchant,
    COUNT(*) AS num_rows,
    MAX(s.amount) AS max_amount
FROM transactions s
JOIN accounts d ON s.account_id = d.id
GROUP BY CUBE (d.status, s.merchant);
