SELECT id, country FROM sessions WHERE browser = 'Firefox' UNION SELECT id, country FROM sessions WHERE device IN ('desktop', 'tablet')
