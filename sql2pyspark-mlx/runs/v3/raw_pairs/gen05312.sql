SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY brand ORDER BY launch_date DESC) AS pos
    FROM products
) AS ranked
WHERE pos <= 3;
