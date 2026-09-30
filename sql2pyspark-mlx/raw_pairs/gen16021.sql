SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rnk
    FROM inventory
) AS ranked
WHERE rnk <= 3;
