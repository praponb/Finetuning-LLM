SELECT id, product_id, CAST(unit_price AS INT) AS unit_price_int FROM order_items WHERE (quantity != 8 AND unit_price > 120) OR order_id IN (15, 16, 22, 23);
