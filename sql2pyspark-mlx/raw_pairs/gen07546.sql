SELECT
    id,
    segment,
    city,
    age,
    CASE WHEN segment IN ('Consumer', 'SMB') THEN 'Core' ELSE 'Other' END AS segment_group,
    CONCAT_WS('/', country, city) AS country_city_key
FROM customers
WHERE credit_limit > 6700
