SELECT id, product_id, units
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) AS rnk
    FROM store_sales
    WHERE store_id = 5
) AS t
WHERE rnk <= 5;
