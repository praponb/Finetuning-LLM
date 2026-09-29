SELECT id, status, shipping_fee, RANK() OVER (PARTITION BY status ORDER BY shipping_fee) shipping_fee_rank FROM orders WHERE amount <= 1310 ORDER BY shipping_fee
