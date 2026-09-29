SELECT unit_price, CAST(quantity AS STRING) AS quantity_num, quantity + unit_price AS line_total
FROM order_items
WHERE unit_price < 120
