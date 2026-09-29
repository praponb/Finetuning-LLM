SELECT
    id,
    product_id,
    units,
    revenue
FROM (
    SELECT
        id,
        product_id,
        units,
        revenue,
        RANK() OVER (PARTITION BY product_id ORDER BY units) AS row_num
    FROM store_sales
) AS r
WHERE row_num <= 2;
