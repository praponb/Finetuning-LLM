SELECT city, unique_citys
FROM (
    SELECT city, COUNT(DISTINCT sqft) AS unique_citys
    FROM stores
    GROUP BY city
) AS x
WHERE unique_citys > 7;
