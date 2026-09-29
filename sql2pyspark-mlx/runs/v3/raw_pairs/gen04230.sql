WITH src AS (
    SELECT *
    FROM order_items
    WHERE order_id = 4 AND (unit_price BETWEEN 140 AND 280 OR unit_price >= 190)
),
product_id_summary AS (
    SELECT product_id, MAX(quantity) AS max_quantity, SUM(unit_price * quantity) AS revenue
    FROM src
    GROUP BY product_id
)
SELECT *
FROM product_id_summary
WHERE max_quantity < 7.1;
