SELECT
    oi.product_id,
    oi.unit_price,
    o.customer_id,
    p.cost,
    DATEDIFF(CURRENT_DATE(), p.launch_date) days_since_launch_date
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
FULL OUTER JOIN products p ON oi.product_id = p.id
WHERE MONTH(p.launch_date) IN (1, 2, 9)
