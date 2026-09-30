SELECT unit_price, quantity, ROUND(unit_price * 1.15, 2) AS unit_price_adjusted FROM order_items WHERE NOT (order_id IN (32, 34, 40) AND quantity = 3)
