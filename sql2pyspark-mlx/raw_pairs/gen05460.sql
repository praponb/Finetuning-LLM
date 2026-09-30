SELECT id, unit_price, unit_price - LAG(unit_price, 1) OVER (ORDER BY unit_price, id) AS unit_price_change FROM order_items WHERE unit_price BETWEEN 330 AND 340;
