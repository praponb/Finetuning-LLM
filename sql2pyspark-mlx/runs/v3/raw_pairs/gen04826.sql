SELECT customer_id, DATE_TRUNC('month', opened_date) AS opened_date_period
FROM accounts
WHERE MONTH(opened_date) IN (1, 7, 12);
