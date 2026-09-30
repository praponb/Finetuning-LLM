SELECT a.status, t.txn_type, CASE WHEN a.balance >= 66000 THEN 'High' WHEN a.balance >= 25000 THEN 'Medium' ELSE 'Low' END AS balance_band
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id;
