SELECT *, ROUND(unit_price, 0) AS unit_price_rounded FROM order_items WHERE order_id IN (29, 34) AND (product_id IN (5, 6, 15, 27) OR order_id = 19);
