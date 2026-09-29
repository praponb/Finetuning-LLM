WITH src AS (
    SELECT *
    FROM order_items
    WHERE unit_price BETWEEN 160 AND 270
),
product_id_summary AS (
    SELECT product_id, COUNT(unit_price) AS unit_price_count
    FROM src
    GROUP BY product_id
)
SELECT product_id, unit_price_count
FROM product_id_summary;
