SELECT credit_limit FROM customers WHERE credit_limit <= 15500 AND (city != 'Tokyo' OR is_active);
