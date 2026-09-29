SELECT
    id,
    rating,
    country,
    LENGTH(country) country_length
FROM suppliers
WHERE country = 'US' AND rating <= 3.5
ORDER BY id DESC
