SELECT supplier_name, rating FROM suppliers WHERE country IN ('JP', 'SG', 'DE') AND country NOT IN ('JP', 'UK', 'TH') AND supplier_name LIKE 'T%'
