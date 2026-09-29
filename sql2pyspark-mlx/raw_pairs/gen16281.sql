SELECT
    id,
    supplier_name,
    city,
    country,
    CASE WHEN country = 'DE' THEN 1 ELSE 0 END is_de
FROM suppliers;
