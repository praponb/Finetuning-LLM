SELECT
    YEAR(a.opened_date) AS opened_year,
    a.account_type,
    COUNT(DISTINCT t.amount) AS unique_txn_types,
    COUNT(*) AS cnt
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
FULL OUTER JOIN customers c ON a.customer_id = c.id
GROUP BY YEAR(a.opened_date), a.account_type
ORDER BY unique_txn_types DESC;
