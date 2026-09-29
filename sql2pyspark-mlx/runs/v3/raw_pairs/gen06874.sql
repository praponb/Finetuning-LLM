SELECT
    a.id,
    a.account_type,
    a.opened_date,
    a.status
FROM accounts a
WHERE YEAR(a.opened_date) >= 2024 AND a.customer_id IN (SELECT id FROM customers WHERE country = 'TH');
