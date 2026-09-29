SELECT
    id,
    rating,
    country,
    supplier_name,
    CONCAT_WS('/', country, city) AS country_city_key,
    FLOOR(rating) AS rating_whole
FROM suppliers
WHERE country = 'TH' AND (city NOT IN ('London', 'New York') OR LENGTH(supplier_name) < 5)
