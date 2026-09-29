SELECT city, country, MAX(rating) - MIN(rating) rating_range FROM suppliers WHERE (city IN ('New York', 'Berlin') AND country = 'TH') OR supplier_name LIKE 'E%' GROUP BY city, country ORDER BY city;
