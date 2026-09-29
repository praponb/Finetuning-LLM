SELECT id, customer_id, country
FROM sessions
WHERE device IN ('mobile', 'desktop') AND (customer_id IS NOT NULL OR country NOT IN ('SG', 'JP', 'UK'))
