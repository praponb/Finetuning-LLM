SELECT
    id,
    rating,
    city,
    country
FROM suppliers
WHERE (country IN ('US', 'UK', 'TH') AND rating < 2.9) OR city NOT IN ('Bangkok', 'Chiang Mai', 'Berlin')
