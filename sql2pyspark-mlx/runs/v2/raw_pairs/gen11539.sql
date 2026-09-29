WITH product_totals AS (
    SELECT product_id, ROUND(AVG(units), 2) AS avg_units
    FROM store_sales
    WHERE sale_date BETWEEN '2024-09-17' AND '2024-11-05'
    GROUP BY product_id
)
SELECT
    p.id,
    p.category,
    p.launch_date,
    pt.avg_units
FROM products p
INNER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.avg_units >= 35
