SELECT
    id,
    country,
    rating,
    city,
    CAST(rating AS INT) AS rating_int
FROM suppliers
