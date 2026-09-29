WITH filtered_order_items AS (
    SELECT *
    FROM order_items
    WHERE quantity < 6
),
Agg AS (
    SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity, COUNT(quantity) AS quantity_count
    FROM filtered_order_items
    GROUP BY product_id
)
SELECT *
FROM Agg
