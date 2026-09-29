SELECT
    id,
    city,
    rating,
    supplier_name
FROM (
    SELECT
        id,
        city,
        rating,
        supplier_name,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
    FROM suppliers
    WHERE city IN ('Tokyo', 'Singapore', 'Bangkok')
) AS t
WHERE rn = 1;
