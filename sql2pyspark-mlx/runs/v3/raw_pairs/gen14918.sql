SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS rnk
    FROM customers
    WHERE LENGTH(email) < 11
) AS ranked
WHERE rnk <= 2
ORDER BY age DESC;
