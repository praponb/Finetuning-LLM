SELECT *
FROM (
    SELECT segment, COUNT(age) AS age_count
    FROM customers
    GROUP BY segment
) AS sub
WHERE age_count > 10;
