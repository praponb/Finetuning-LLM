SELECT
    id,
    city,
    rating,
    country,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) dense_rnk,
    ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating, id) rating_row
FROM suppliers;
