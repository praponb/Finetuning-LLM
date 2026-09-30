SELECT id, status, amount, COALESCE(method, 'unassigned') AS method_filled FROM payments;
