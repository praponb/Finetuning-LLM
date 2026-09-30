SELECT
    id,
    rating,
    supplier_name,
    city
FROM suppliers
WHERE rating > (SELECT AVG(rating) FROM suppliers);
