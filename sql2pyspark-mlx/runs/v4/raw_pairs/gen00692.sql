SELECT ROUND(AVG(rating), 2) AS avg_rating, COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count
FROM suppliers
WHERE country = 'US' AND supplier_name IS NOT NULL;
