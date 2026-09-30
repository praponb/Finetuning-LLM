SELECT
    a.id,
    a.status,
    a.account_type,
    a.opened_date,
    c.name,
    LAST_DAY(a.opened_date) AS opened_date_month_end
FROM accounts a
JOIN customers c ON a.customer_id = c.id
WHERE NOT (a.account_type = 'CREDIT' AND a.balance <= 23000)
