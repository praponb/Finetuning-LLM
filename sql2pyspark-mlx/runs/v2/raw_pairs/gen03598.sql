SELECT a.customer_id, a.opened_date, t.total_amount
FROM accounts a
JOIN (SELECT account_id, SUM(amount) total_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.total_amount > 19000
ORDER BY t.total_amount DESC, customer_id DESC
