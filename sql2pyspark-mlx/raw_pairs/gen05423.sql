SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) / 365 AS years_since_opened_date
FROM accounts
WHERE account_type IN ('SAVINGS', 'LOAN', 'CREDIT') AND opened_date BETWEEN '2024-03-30' AND '2024-09-01'
ORDER BY 2 DESC
