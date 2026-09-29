SELECT country FROM suppliers WHERE country NOT IN ('SG', 'TH') UNION ALL SELECT country FROM suppliers WHERE LENGTH(supplier_name) > 11
