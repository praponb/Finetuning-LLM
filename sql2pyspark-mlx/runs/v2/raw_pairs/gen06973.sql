SELECT a.opened_date, a.account_type, t.amount
FROM accounts a
LEFT OUTER JOIN transactions t ON t.account_id = a.id
