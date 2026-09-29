SELECT id, city FROM suppliers WHERE country = 'DE' UNION ALL SELECT id, city FROM suppliers WHERE country IN ('UK', 'DE', 'TH') ORDER BY id
