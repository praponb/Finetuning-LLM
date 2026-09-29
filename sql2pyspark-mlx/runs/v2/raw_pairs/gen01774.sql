SELECT *, ADD_MONTHS(opened_date, 6) AS opened_date_plus_months, CONCAT(account_type, ' - ', status) AS account_type_status
FROM accounts
WHERE account_type = 'CHECKING' AND YEAR(opened_date) = 2025
ORDER BY id DESC, status DESC
LIMIT 3;
