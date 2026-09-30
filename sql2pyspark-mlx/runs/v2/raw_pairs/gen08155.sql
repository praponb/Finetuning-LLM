SELECT a.customer_id, t.txn_type
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id;
