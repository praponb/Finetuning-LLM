SELECT
    product_id,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    AVG(quantity) AS avg_quantity,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
GROUP BY product_id
HAVING MAX(unit_price) - MIN(unit_price) >= 58
ORDER BY 2 DESC
