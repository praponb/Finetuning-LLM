SELECT a.account_type, t.amount, MONTH(a.opened_date) opened_date_month
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE t.txn_time >= '2023-09-01';
