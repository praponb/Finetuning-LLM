SELECT supplier_name, city, country FROM suppliers WHERE country IN ('UK', 'TH', 'DE') AND rating BETWEEN 1.8 AND 4.0 ORDER BY supplier_name ASC
