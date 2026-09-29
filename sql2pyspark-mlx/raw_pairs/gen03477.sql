SELECT id, sale_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date ASC NULLS LAST, id ASC) AS rn
    FROM store_sales
) x
WHERE rn = 1;
