SELECT c.email, a.balance, ADD_MONTHS(a.opened_date, 1) AS opened_date_plus_months FROM customers c LEFT JOIN accounts a ON a.customer_id = c.id WHERE c.city != 'Bangkok' ORDER BY a.balance DESC
