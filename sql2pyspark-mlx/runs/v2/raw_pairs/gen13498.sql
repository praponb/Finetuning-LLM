SELECT city, supplier_name, CASE WHEN rating >= 4.5 THEN 'High' WHEN rating >= 1.7 THEN 'Medium' ELSE 'Low' END AS rating_band
FROM suppliers
WHERE country = 'US' AND rating < 3.1
ORDER BY rating_band ASC;
