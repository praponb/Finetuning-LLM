SELECT
    id,
    city,
    rating,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rating_row
FROM suppliers
ORDER BY id DESC;
