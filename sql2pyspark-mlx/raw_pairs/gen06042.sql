SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS rn
    FROM products
) AS t
WHERE rn <= 2;
