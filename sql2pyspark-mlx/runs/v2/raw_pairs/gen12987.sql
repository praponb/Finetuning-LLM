SELECT rating, supplier_name, country FROM suppliers WHERE country <> 'JP' AND country IN ('SG', 'UK') AND city = 'Chiang Mai' ORDER BY rating ASC;
