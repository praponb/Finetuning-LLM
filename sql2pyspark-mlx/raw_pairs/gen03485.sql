SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rnk
    FROM suppliers
) AS ranked
WHERE rnk <= 5;
