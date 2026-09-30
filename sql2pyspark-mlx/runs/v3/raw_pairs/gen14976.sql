SELECT
    id,
    product_id,
    sale_date,
    revenue
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS pos
    FROM store_sales
) AS r
WHERE pos <= 1
