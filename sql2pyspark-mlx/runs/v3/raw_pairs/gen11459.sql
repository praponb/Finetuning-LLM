SELECT id, unit_price, ROUND(unit_price * 1.15, 2) AS unit_price_scaled FROM order_items ORDER BY id DESC LIMIT 20
