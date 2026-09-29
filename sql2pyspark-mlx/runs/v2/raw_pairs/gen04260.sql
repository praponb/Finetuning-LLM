SELECT account_type, ADD_MONTHS(opened_date, 12) opened_date_plus_months
FROM accounts
WHERE opened_date > '2025-04-03' AND opened_date < '2025-03-01'
