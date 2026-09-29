SELECT id, status, method, amount, CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed FROM payments WHERE amount >= 650 AND amount <= 560;
