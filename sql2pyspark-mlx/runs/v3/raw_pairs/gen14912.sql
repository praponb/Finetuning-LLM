SELECT
    i.store_id,
    ss.sale_date,
    ss.product_id,
    ss.units,
    ROUND(i.quantity * 1.07, 2) AS adjusted_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (1, 3, 5) AND ss.store_id IN (1, 2, 3, 6)
ORDER BY product_id, store_id DESC;
