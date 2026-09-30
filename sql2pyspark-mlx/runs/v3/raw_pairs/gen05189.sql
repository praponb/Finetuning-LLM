SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) rn
    FROM customers
) t
WHERE rn = 1
ORDER BY 4;
