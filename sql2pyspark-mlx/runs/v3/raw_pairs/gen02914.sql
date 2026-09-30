SELECT country, london_count
FROM (
    SELECT country, COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count
    FROM suppliers
    WHERE city NOT IN ('Tokyo', 'Chiang Mai', 'New York')
    GROUP BY country
) AS x
WHERE london_count >= 7
