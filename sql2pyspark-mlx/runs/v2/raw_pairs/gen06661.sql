SELECT id, status, CAST(amount AS INT) AS amount_int, QUARTER(paid_at) AS paid_at_quarter FROM payments WHERE paid_at >= '2024-07-13' AND order_id = 29
