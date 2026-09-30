SELECT
    order_id,
    product_id,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    SUM(unit_price * quantity) AS revenue
FROM order_items
GROUP BY order_id, product_id, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
HAVING ROUND(AVG(unit_price), 2) > 33
ORDER BY revenue, order_id, product_id, quantity_bucket
LIMIT 3;
