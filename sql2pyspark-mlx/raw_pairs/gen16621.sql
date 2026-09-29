SELECT a.id, a.customer_id, c.credit_limit, DAY(a.opened_date) AS opened_date_day FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id
