SELECT id, rating, supplier_name, country FROM suppliers WHERE (country IN ('UK', 'SG', 'TH') AND country <> 'TH') OR country = 'UK';
