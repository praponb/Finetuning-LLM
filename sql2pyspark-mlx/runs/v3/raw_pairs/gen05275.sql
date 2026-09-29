SELECT shipping_fee, amount, discount, CAST(amount AS INT) AS amount_int FROM orders WHERE order_date > '2024-12-28'
