SELECT *, UPPER(supplier_name) AS supplier_name_upper, ROUND(rating, 2) AS rating_rounded FROM suppliers WHERE NOT (rating < 3.5 AND (country IN ('SG', 'DE', 'US') OR country IN ('DE', 'US')))
