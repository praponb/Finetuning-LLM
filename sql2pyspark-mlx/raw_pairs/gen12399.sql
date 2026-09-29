WITH valid_order_items AS (
    SELECT *
    FROM order_items
    WHERE quantity < 6 AND order_id IN (8, 11, 35) AND unit_price BETWEEN 310 AND 330
),
OrderIdSummary AS (
    SELECT
        order_id,
        CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        SUM(quantity * unit_price) AS revenue,
        COUNT(*) AS num_order_items
    FROM valid_order_items
    GROUP BY order_id, CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END
)
SELECT
    order_id,
    unit_price_bucket,
    revenue,
    num_order_items
FROM OrderIdSummary
WHERE num_order_items > 5
