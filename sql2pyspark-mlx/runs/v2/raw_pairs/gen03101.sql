SELECT sessions.browser, sessions.country, customers.age, customers.credit_limit FROM sessions LEFT JOIN customers ON customers.id = sessions.customer_id ORDER BY country DESC;
