SELECT
    id,
    city,
    supplier_name,
    ROUND(rating, 0) AS rating_rounded
FROM suppliers
WHERE country <> 'UK' AND country NOT IN ('DE', 'TH') AND country IN ('TH', 'JP');
