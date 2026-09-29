WITH src AS (
    SELECT *
    FROM order_items
    WHERE order_id IN (7, 28) AND (product_id IN (16, 32) OR quantity >= 6)
),
summary AS (
    SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM src
    GROUP BY product_id
)
SELECT *
FROM summary
WHERE stddev_unit_price > 66;
