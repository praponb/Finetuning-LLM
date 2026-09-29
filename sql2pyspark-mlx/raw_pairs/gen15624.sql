SELECT
    order_id,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE order_id IN (11, 21, 30)
GROUP BY order_id, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
HAVING stddev_unit_price < 58;
