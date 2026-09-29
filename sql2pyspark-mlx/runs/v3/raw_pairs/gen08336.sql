SELECT
    a.id,
    a.account_type,
    a.balance,
    t.amount_count,
    t.amount_range
FROM accounts a
JOIN (SELECT account_id, COUNT(amount) AS amount_count, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id;
