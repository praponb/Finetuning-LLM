SELECT
    city,
    SUM(rating) FILTER (WHERE city = 'Berlin') AS berlin_total_rating,
    STDDEV_SAMP(rating) AS rating_stddev,
    AVG(rating) FILTER (WHERE country = 'SG') AS sg_avg_rating
FROM suppliers
GROUP BY city
ORDER BY sg_avg_rating NULLS FIRST;
