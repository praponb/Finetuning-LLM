SELECT
    id,
    product_id,
    store_id,
    units,
    sale_date,
    RANK() OVER (PARTITION BY product_id, store_id ORDER BY units) AS rnk,
    NTILE(10) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS bucket
FROM store_sales
ORDER BY units DESC;
