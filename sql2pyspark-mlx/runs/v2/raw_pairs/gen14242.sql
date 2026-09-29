SELECT *, CAST(rating AS INT) AS rating_int, CONCAT(country, ' - ', supplier_name) AS country_supplier_name FROM suppliers ORDER BY rating DESC, country ASC, id ASC LIMIT 5;
