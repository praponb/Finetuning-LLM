SELECT id, country, supplier_name, ROUND(rating * 1.1, 2) AS rating_adjusted FROM suppliers WHERE country = 'JP' OR city IN ('New York', 'London', 'Chiang Mai') ORDER BY id, rating_adjusted
