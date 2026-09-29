SELECT a.status, t.amazon_count
FROM accounts a
JOIN (SELECT account_id, COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amazon_count > 6
ORDER BY status DESC;
