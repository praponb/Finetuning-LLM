SELECT *
FROM suppliers
WHERE country IN ('TH', 'JP') AND rating IS NOT NULL
