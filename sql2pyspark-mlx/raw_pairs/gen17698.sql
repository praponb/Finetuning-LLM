SELECT YEAR(opened_date) AS opened_year, status, MAX(opened_date) AS last_opened_date FROM accounts GROUP BY YEAR(opened_date), status;
