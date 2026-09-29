SELECT id, status, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label
FROM accounts
WHERE opened_date < '2024-08-01'
