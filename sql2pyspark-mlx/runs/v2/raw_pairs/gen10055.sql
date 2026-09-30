WITH src AS (
    SELECT *
    FROM order_items
    WHERE quantity BETWEEN 5 AND 8 AND unit_price BETWEEN 120 AND 200 AND unit_price >= 200
),
agg AS (
    SELECT order_id, SUM(unit_price * quantity) revenue, ROUND(STDDEV(unit_price), 2) stddev_unit_price
    FROM src
    GROUP BY order_id
)
SELECT order_id, revenue, stddev_unit_price
FROM agg
