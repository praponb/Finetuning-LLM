SELECT status, MIN(amount) AS min_amount FROM payments WHERE order_id = 29 GROUP BY status HAVING MIN(amount) > 1030
