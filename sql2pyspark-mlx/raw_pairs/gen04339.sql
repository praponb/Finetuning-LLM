WITH product_totals AS (
    SELECT product_id, COUNT(DISTINCT revenue) AS unique_products, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE store_id IN (4, 5, 6)
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    pt.unique_products,
    pt.stddev_units
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id
ORDER BY unique_products DESC, id
