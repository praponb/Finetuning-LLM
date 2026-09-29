SELECT
    id,
    opened_date,
    status,
    customer_id
FROM accounts
WHERE opened_date > (SELECT MAX(opened_date) FROM accounts)
ORDER BY status, customer_id DESC;
