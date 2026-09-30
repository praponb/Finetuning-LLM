SELECT sessions.browser, sessions.country, customers.email FROM sessions JOIN customers ON customers.id = sessions.customer_id WHERE sessions.customer_id = 6;
