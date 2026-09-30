SELECT *, ROUND(rating * 1.07, 2) AS rating_adjusted, CONCAT(supplier_name, ' - ', city) AS supplier_name_city FROM suppliers WHERE city NOT IN ('Berlin', 'Tokyo')
