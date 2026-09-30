SELECT
    a.opened_date,
    a.account_type,
    a.balance,
    c.segment,
    c.name,
    c.city
FROM accounts a
JOIN customers c ON c.id = a.customer_id
