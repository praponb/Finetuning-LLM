SELECT
    a.id,
    a.customer_id,
    a.opened_date,
    t.num_transactions
FROM accounts a
LEFT OUTER JOIN (SELECT account_id, COUNT(*) AS num_transactions
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY t.num_transactions DESC;
