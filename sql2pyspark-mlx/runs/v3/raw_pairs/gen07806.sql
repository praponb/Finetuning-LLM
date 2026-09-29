SELECT
    id,
    store_id,
    units,
    RANK() OVER (PARTITION BY store_id ORDER BY units) AS rnk
FROM store_sales
WHERE store_id = 3
ORDER BY id DESC
