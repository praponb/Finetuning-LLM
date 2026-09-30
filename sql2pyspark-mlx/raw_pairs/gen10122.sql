SELECT a.opened_date, a.balance, a.status, t.amount FROM accounts a LEFT OUTER JOIN transactions t ON a.id = t.account_id ORDER BY opened_date, balance DESC;
