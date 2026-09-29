SELECT id, amount, store_id, amount + shipping_fee AS amount_with_shipping FROM orders WHERE amount <= 1350;
