WITH FilteredOrderItems AS (
    SELECT *
    FROM order_items
    WHERE NOT (quantity > 7 AND quantity = 6)
),
ProductIdSummary AS (
    SELECT product_id, CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END AS unit_price_bucket, SUM(quantity * unit_price) AS revenue
    FROM FilteredOrderItems
    GROUP BY product_id, CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM ProductIdSummary
WHERE revenue >= 23
ORDER BY unit_price_bucket, product_id
LIMIT 1;
