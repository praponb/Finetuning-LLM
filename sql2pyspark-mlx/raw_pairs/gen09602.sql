SELECT sessions.browser, customers.signup_date FROM sessions LEFT OUTER JOIN customers ON sessions.customer_id = customers.id;
