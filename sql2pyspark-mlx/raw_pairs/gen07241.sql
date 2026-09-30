SELECT
    t.id,
    t.merchant,
    t.amount,
    a.opened_date,
    c.credit_limit,
    GREATEST(c.credit_limit, c.age) greatest_credit_limit_age
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON c.id = a.customer_id
WHERE c.segment NOT IN ('Consumer', 'SMB')
ORDER BY amount DESC;
