SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS pos
    FROM store_sales
) AS t
WHERE pos <= 3
ORDER BY product_id DESC;
