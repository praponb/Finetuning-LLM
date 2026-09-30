SELECT
    id,
    supplier_name,
    city,
    rating
FROM suppliers
QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) = 1;
