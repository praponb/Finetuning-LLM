SELECT status, paid_at, amount FROM payments WHERE NOT (status NOT IN ('REFUNDED', 'SUCCESS', 'FAILED') AND amount <= 1430)
