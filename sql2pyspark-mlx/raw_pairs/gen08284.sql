WITH product_totals AS (
    SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM order_items
    GROUP BY product_id
)
SELECT p.id, p.category, pt.stddev_unit_price
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id;
