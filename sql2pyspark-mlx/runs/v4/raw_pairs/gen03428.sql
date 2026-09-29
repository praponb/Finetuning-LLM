SELECT id, opened_date, balance, DATE_TRUNC('year', opened_date) AS opened_date_period FROM accounts WHERE opened_date BETWEEN '2024-06-01' AND '2025-02-01';
