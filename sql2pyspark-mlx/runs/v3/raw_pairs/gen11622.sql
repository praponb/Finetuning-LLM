SELECT id, country, customer_id FROM sessions WHERE customer_id = 21 UNION ALL SELECT id, country, customer_id FROM sessions WHERE country IN ('US', 'UK')
