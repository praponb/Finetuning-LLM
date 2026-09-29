SELECT status, MIN(amount) AS min_amount FROM payments WHERE order_id IS NULL AND amount > 610 AND amount BETWEEN 870 AND 1250 GROUP BY status HAVING MIN(amount) > 1520;
