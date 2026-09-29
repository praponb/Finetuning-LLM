SELECT accounts.id, accounts.customer_id, t.amount_count
FROM accounts
INNER JOIN (SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id) t ON accounts.id = t.account_id
WHERE t.amount_count > 6
ORDER BY id DESC
