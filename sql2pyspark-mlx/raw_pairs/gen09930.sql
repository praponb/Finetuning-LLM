SELECT country FROM sessions WHERE customer_id IN (14, 16, 18) UNION ALL SELECT country FROM sessions WHERE device = 'desktop' ORDER BY country
