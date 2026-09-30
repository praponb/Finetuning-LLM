WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE order_id IN (6, 32) AND unit_price < 220
),
agg AS (
    SELECT
        product_id,
        order_id,
        ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
        SUM(unit_price * quantity) AS revenue
    FROM recent_order_items
    GROUP BY product_id, order_id
)
SELECT product_id, stddev_unit_price, revenue
FROM agg
WHERE revenue >= 73;
