WITH product_summary AS (
    SELECT product_id, ROUND(STDDEV(units), 2) AS stddev_units, MAX(units) AS max_units
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    ps.stddev_units,
    ps.max_units
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
ORDER BY ps.stddev_units;
