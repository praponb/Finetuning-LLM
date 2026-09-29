SELECT order_id, SUM(unit_price) AS total_unit_price, ROUND(SUM(unit_price) * 100.0 / SUM(SUM(unit_price)) OVER (), 2) AS pct_of_total
FROM order_items
GROUP BY order_id
ORDER BY pct_of_total, order_id DESC;
