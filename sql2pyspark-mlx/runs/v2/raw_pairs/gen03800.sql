SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC) AS rnk
    FROM products
) AS ranked
WHERE rnk <= 2;
