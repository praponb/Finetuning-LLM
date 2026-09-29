SELECT city, CASE WHEN age >= 56 THEN 'High' WHEN age >= 25 THEN 'Medium' ELSE 'Low' END AS age_band
FROM customers
WHERE LENGTH(name) >= 10 OR email LIKE '%@company.co'
ORDER BY city DESC, age_band
