SELECT
    product_id,
    SUM(quantity * unit_price) AS revenue,
    COUNT(*) AS total_count,
    COUNT(*) AS cnt
FROM order_items
WHERE product_id IS NULL
GROUP BY product_id;
