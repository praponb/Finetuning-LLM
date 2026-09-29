SELECT
    ss.store_id,
    QUARTER(ss.sale_date) AS sale_quarter,
    SUM(i.quantity) AS total_quantity,
    MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (2, 4, 5, 6)
GROUP BY ss.store_id, QUARTER(ss.sale_date)
ORDER BY ss.store_id DESC, total_quantity DESC;
