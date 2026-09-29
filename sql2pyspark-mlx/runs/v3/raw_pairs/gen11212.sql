SELECT unit_price, order_id, product_id, ROUND(unit_price / quantity, 2) line_total FROM order_items WHERE unit_price >= 110 AND quantity BETWEEN 5 AND 7;
