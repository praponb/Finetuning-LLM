SELECT
    product_id,
    store_id,
    CASE WHEN quantity >= 257 THEN 'high' WHEN quantity >= 48 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    SUM(quantity) AS total_quantity
FROM inventory
GROUP BY 1, 2, 3;
