SELECT a.id, a.account_type, a.balance, t.min_amount FROM accounts a INNER JOIN (SELECT account_id, MIN(amount) AS min_amount FROM transactions GROUP BY account_id) t ON a.id = t.account_id
