SELECT *, CAST(unit_price AS INT) AS unit_price_int FROM order_items WHERE unit_price IS NOT NULL AND unit_price BETWEEN 160 AND 330 AND quantity = 3;
