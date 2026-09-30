SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date FROM accounts WHERE opened_date BETWEEN '2023-11-20' AND '2025-01-29';
