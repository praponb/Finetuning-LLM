SELECT id, city FROM suppliers WHERE country = 'US' UNION SELECT id, city FROM suppliers WHERE city IN ('Chiang Mai', 'Berlin', 'Tokyo') ORDER BY city
