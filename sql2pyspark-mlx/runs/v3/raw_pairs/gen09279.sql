SELECT
    a.status,
    a.account_type,
    a.customer_id,
    t.merchant,
    t.is_flagged
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE a.status = 'OPEN';
