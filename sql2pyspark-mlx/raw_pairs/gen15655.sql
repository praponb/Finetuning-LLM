SELECT product_id, quantity_bucket, revenue
FROM (
    SELECT product_id, CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END quantity_bucket, SUM(unit_price * quantity) revenue
    FROM order_items
    WHERE unit_price >= 150
    GROUP BY product_id, CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
) agg
WHERE revenue > 21
ORDER BY product_id DESC;
