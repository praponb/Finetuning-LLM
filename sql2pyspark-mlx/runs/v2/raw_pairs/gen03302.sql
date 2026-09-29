SELECT *
FROM (
    SELECT
        id,
        city,
        rating,
        supplier_name,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) row_num
    FROM suppliers
    WHERE city NOT IN ('Singapore', 'Bangkok', 'London')
) r
WHERE row_num <= 1
ORDER BY rating DESC
