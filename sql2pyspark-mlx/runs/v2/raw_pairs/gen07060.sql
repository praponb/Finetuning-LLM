SELECT
    city,
    country,
    CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket,
    COUNT(*) AS num_suppliers,
    MAX(rating) AS max_rating
FROM suppliers
WHERE supplier_name LIKE 'T%' AND supplier_name NOT LIKE 'To%'
GROUP BY city, country, CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END
