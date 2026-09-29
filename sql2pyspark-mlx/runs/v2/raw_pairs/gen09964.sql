SELECT *, amount * discount AS amount_times_discount
FROM orders
WHERE channel IS NOT NULL;
