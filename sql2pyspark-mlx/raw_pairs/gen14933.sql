SELECT
    id,
    product_id,
    units,
    revenue
FROM (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS pos
    FROM store_sales
) AS r
WHERE pos <= 2
