SELECT product_id, order_id, MIN(unit_price) AS min_unit_price
FROM order_items
WHERE product_id = 13
GROUP BY product_id, order_id
ORDER BY product_id DESC;
