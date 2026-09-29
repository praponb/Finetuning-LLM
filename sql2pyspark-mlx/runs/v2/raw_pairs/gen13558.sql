SELECT order_id, unit_price, CAST(quantity AS DOUBLE) AS quantity_str
FROM order_items
WHERE unit_price <= 280 AND quantity != 6;
