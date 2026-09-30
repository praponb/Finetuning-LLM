SELECT
    product_id,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS cnt,
    SUM(SUM(quantity)) OVER (ORDER BY product_id) AS cumulative_quantity
FROM inventory
WHERE store_id IN (2, 5)
GROUP BY product_id
ORDER BY product_id DESC, cnt ASC
