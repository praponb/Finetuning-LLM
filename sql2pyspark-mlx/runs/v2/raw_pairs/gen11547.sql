WITH product_summary AS (
    SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM order_items
    GROUP BY product_id
)
SELECT p.brand, p.launch_date, ps.stddev_unit_price
FROM products p
JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.stddev_unit_price >= 62;
