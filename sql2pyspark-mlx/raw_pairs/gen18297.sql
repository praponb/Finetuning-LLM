SELECT product_id, max_unit_price, revenue
FROM (
    SELECT product_id, MAX(unit_price) AS max_unit_price, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY product_id
) AS x
WHERE max_unit_price >= 270
ORDER BY max_unit_price, product_id DESC;
