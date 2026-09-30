SELECT *, ROUND(quantity / unit_price, 2) AS line_total, COALESCE(unit_price, 0) AS unit_price_filled
FROM order_items
WHERE unit_price > 210 AND order_id IN (21, 22, 23, 26) AND quantity <= 6
