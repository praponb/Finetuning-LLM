SELECT
    id,
    city,
    rating,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) rating_row
FROM suppliers
WHERE rating < 4.1
ORDER BY rating DESC
