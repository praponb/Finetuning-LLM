WITH src AS (
    SELECT *, CASE WHEN quantity >= 9 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM order_items
    WHERE product_id IN (10, 12, 19, 27)
),
product_id_summary AS (
    SELECT product_id, AVG(quantity) AS avg_quantity
    FROM src
    GROUP BY product_id
)
SELECT product_id, avg_quantity
FROM product_id_summary
ORDER BY product_id DESC, avg_quantity DESC
