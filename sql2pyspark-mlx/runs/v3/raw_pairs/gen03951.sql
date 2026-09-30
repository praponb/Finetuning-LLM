SELECT
    t.merchant,
    t.txn_type,
    a.opened_date,
    c.email,
    c.is_active,
    ADD_MONTHS(a.opened_date, 12) AS opened_date_plus_months
FROM transactions t
JOIN accounts a ON t.account_id = a.id
JOIN customers c ON c.id = a.customer_id;
