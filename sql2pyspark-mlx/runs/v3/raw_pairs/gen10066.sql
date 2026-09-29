SELECT customers.age, customers.signup_date, sessions.customer_id
FROM customers
LEFT JOIN sessions ON customers.id = sessions.customer_id
WHERE customers.country IN ('US', 'DE', 'UK')
ORDER BY customers.signup_date DESC, sessions.customer_id DESC
