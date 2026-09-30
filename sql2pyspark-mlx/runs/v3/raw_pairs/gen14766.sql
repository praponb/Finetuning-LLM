SELECT a.opened_date, t.merchant, t.amount
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id;
