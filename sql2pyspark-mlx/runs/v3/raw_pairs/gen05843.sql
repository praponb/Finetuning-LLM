SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS rnk
    FROM customers
    WHERE age BETWEEN 52 AND 61
) AS ranked
WHERE rnk = 1
ORDER BY name DESC, signup_date DESC
