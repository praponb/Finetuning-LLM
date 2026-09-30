SELECT
    supplier_name,
    rating,
    country,
    ROUND(rating, 1) AS rating_rounded
FROM suppliers
WHERE country = 'SG'
ORDER BY rating_rounded;
