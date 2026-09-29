SELECT
    id,
    product_id,
    units,
    sale_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) AS pos
    FROM store_sales
) AS r
WHERE pos <= 3
ORDER BY 1 DESC;
