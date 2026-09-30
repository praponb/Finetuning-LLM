SELECT
    a.balance,
    a.status,
    t.unique_merchants,
    t.total_count
FROM accounts a
LEFT JOIN (SELECT account_id, COUNT(DISTINCT amount) AS unique_merchants, COUNT(*) AS total_count
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY a.status ASC;
