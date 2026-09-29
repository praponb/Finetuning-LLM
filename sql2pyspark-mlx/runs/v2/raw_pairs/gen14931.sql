SELECT order_id, quantity, CAST(unit_price AS INT) AS unit_price_int
FROM order_items;
