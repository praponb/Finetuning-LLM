SELECT
    units,
    revenue,
    store_id,
    CAST(units AS STRING) AS units_num
FROM store_sales;
