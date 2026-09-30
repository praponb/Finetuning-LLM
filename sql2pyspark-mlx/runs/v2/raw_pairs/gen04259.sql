SELECT id, customer_id, opened_date, balance, DATE_SUB(opened_date, 70) AS opened_date_minus_70d FROM accounts WHERE (customer_id IN (4, 38) AND status IS NOT NULL) OR status IS NOT NULL;
