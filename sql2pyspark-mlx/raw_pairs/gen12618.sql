SELECT a.id, a.customer_id, t.amount_range
FROM accounts a
INNER JOIN (SELECT account_id, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amount_range >= 45
