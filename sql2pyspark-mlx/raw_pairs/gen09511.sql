WITH product_totals AS (
    SELECT product_id, MAX(revenue) - MIN(revenue) AS revenue_range, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    GROUP BY product_id
)
SELECT p.price, pt.revenue_range, pt.stddev_units
FROM products p
INNER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.stddev_units >= 37;
