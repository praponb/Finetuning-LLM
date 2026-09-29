SELECT
    a.opened_date,
    a.customer_id,
    c.country,
    c.name
FROM accounts a
LEFT OUTER JOIN customers c ON c.id = a.customer_id;
