SELECT
    id,
    city,
    rating,
    supplier_name,
    country,
    LAST_VALUE(supplier_name) OVER (PARTITION BY city ORDER BY rating DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_supplier_name,
    ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating, id) AS row_num
FROM suppliers
WHERE rating <= 2.0;
