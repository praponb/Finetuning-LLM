SELECT *, CONCAT(supplier_name, ' ', country) AS supplier_name_country FROM suppliers WHERE rating <= 3.6 AND city IN ('London', 'Tokyo') ORDER BY rating DESC;
