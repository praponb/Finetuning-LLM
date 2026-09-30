SELECT
    c.credit_limit,
    c.is_active,
    a.status,
    a.balance,
    a.customer_id,
    LENGTH(a.account_type) AS account_type_length
FROM customers c
LEFT OUTER JOIN accounts a ON c.id = a.customer_id
WHERE c.segment != 'SMB';
