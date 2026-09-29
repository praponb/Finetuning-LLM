SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC) AS rnk
    FROM products
) AS ranked
WHERE rnk <= 3
