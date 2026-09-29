SELECT a.opened_date, a.customer_id, t.account_id, t.merchant, t.txn_time FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id WHERE YEAR(a.opened_date) >= 2025
