SELECT
    p.status,
    s.merchant,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.amount), 2) AS total_amount
FROM transactions s
LEFT JOIN accounts p ON s.account_id = p.id
GROUP BY CUBE (p.status, s.merchant)
ORDER BY merchant DESC NULLS LAST
