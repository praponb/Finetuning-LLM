SELECT unit_price AS value FROM order_items WHERE unit_price IS NOT NULL EXCEPT SELECT amount AS value FROM transactions WHERE amount IS NOT NULL ORDER BY value;
