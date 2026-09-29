WITH Src AS (
    SELECT *, CEIL(quantity) quantity_whole
    FROM order_items
    WHERE quantity > 6
),
order_items_stats AS (
    SELECT order_id, COUNT(DISTINCT unit_price) unique_products
    FROM Src
    GROUP BY order_id
)
SELECT *
FROM order_items_stats
ORDER BY order_id DESC;
