WITH src AS (
    SELECT *, ROUND(quantity, 1) AS quantity_rounded
    FROM order_items
    WHERE unit_price >= 140
),
summary AS (
    SELECT product_id, order_id, MIN(unit_price) AS min_unit_price
    FROM src
    GROUP BY product_id, order_id
)
SELECT product_id, order_id
FROM summary
ORDER BY order_id DESC, product_id
LIMIT 5;
