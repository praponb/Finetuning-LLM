SELECT product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
WHERE quantity BETWEEN 3 AND 7
GROUP BY product_id
ORDER BY 3 ASC, unit_price_range DESC, product_id ASC
LIMIT 10
