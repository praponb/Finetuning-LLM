SELECT
    segment,
    city,
    country,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count
FROM customers
GROUP BY segment, city, country;
