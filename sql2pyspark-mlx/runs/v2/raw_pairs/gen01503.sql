SELECT id, opened_date, customer_id, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label FROM accounts ORDER BY opened_date_label DESC
