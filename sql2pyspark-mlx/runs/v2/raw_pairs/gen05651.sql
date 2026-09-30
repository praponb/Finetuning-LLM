SELECT browser FROM sessions WHERE device = 'desktop' UNION ALL SELECT browser FROM sessions WHERE customer_id = 34 ORDER BY browser
