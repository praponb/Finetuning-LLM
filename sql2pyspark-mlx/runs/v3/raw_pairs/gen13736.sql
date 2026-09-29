SELECT
    ss.store_id,
    ss.product_id,
    MAX(i.quantity) AS max_quantity,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock < '2023-07-01'
GROUP BY ss.store_id, ss.product_id
ORDER BY product_id DESC, store_id
LIMIT 10
