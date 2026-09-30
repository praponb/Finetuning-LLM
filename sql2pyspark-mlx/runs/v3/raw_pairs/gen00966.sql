SELECT
    id,
    rating,
    city,
    country,
    ROW_NUMBER() OVER (ORDER BY rating, id) AS rn,
    DENSE_RANK() OVER (PARTITION BY city, country ORDER BY rating) AS dense_rnk
FROM suppliers
