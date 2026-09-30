SELECT id, country, customer_id
FROM sessions
WHERE device IN ('tablet', 'desktop', 'mobile') AND country != 'SG'
