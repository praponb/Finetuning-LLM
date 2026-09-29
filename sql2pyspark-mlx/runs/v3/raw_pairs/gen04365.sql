SELECT shipping_fee, status, amount, amount + discount AS amount_plus_discount, INITCAP(TRIM(channel)) AS channel_title FROM orders WHERE channel = 'store'
