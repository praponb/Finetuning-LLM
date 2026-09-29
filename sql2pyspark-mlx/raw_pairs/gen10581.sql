SELECT a.balance, a.opened_date, t.txn_type, t.account_id, t.amount FROM accounts a INNER JOIN transactions t ON a.id = t.account_id WHERE a.customer_id IN (6, 18) AND YEAR(a.opened_date) = 2023;
