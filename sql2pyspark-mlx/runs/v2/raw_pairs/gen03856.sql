SELECT *, CASE WHEN age >= 61 THEN 'High' WHEN age >= 26 THEN 'Medium' ELSE 'Low' END AS age_band
FROM customers
ORDER BY credit_limit;
