SELECT
    id,
    product_id,
    CASE WHEN quantity >= 242 THEN 'High' WHEN quantity >= 92 THEN 'Medium' ELSE 'Low' END AS quantity_band,
    CAST(quantity AS STRING) AS quantity_num
FROM inventory
ORDER BY quantity_band, id
LIMIT 20;
