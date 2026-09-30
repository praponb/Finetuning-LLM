SELECT a.account_type, c.signup_date
FROM accounts a
LEFT JOIN customers c ON c.id = a.customer_id
WHERE c.segment IN ('SMB', 'Corporate') AND a.customer_id IN (7, 8, 10, 31);
