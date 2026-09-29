SELECT id, amount, CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END is_success FROM payments
