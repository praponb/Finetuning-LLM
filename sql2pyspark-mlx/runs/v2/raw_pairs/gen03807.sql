SELECT id, amount, CAST(shipping_fee AS INT) AS shipping_fee_int FROM orders WHERE amount IS NULL;
