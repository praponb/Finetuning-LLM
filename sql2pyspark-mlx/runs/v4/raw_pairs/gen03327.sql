SELECT
    product_id,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    COUNT(*) AS num_order_items,
    COUNT(*) AS total_count
FROM order_items
GROUP BY product_id
ORDER BY product_id ASC, total_count DESC;
