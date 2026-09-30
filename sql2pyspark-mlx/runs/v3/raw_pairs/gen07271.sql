SELECT
    id,
    revenue,
    product_id,
    CAST(units AS DOUBLE) AS units_num,
    ROUND(units, 2) AS units_rounded
FROM store_sales;
