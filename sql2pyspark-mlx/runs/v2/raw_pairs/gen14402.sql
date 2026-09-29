WITH ValidOrderItems AS (
    SELECT *, quantity * unit_price AS line_total
    FROM order_items
    WHERE unit_price > 270
),
Summary AS (
    SELECT product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM ValidOrderItems
    GROUP BY product_id
)
SELECT product_id, unit_price_range
FROM Summary
WHERE unit_price_range >= 67
ORDER BY unit_price_range DESC, product_id
LIMIT 50
