SELECT *, CASE WHEN rating >= 3.9 THEN 'High' WHEN rating >= 1.5 THEN 'Medium' ELSE 'Low' END AS rating_band FROM suppliers WHERE city != 'Chiang Mai' AND rating > 3.3 ORDER BY supplier_name
