SELECT
    MONTH(o.order_date) order_month,
    o.status,
    ROUND(STDDEV(oi.quantity), 2) stddev_quantity,
    COUNT(oi.quantity) quantity_count
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON o.id = oi.order_id
GROUP BY MONTH(o.order_date), o.status
ORDER BY order_month DESC;
