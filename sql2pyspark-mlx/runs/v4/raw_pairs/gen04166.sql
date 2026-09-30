SELECT *, INITCAP(TRIM(channel)) channel_title, CAST(shipping_fee AS INT) shipping_fee_int FROM orders
