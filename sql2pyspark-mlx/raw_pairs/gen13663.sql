SELECT shipping_fee, status, discount, CAST(amount AS INT) AS amount_int FROM orders WHERE shipping_fee > 25 AND (YEAR(order_date) = 2025 OR discount BETWEEN 0.24 AND 0.25) ORDER BY shipping_fee
