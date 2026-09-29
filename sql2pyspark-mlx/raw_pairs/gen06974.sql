SELECT merchant FROM transactions WHERE merchant IS NOT NULL EXCEPT SELECT full_name FROM employees WHERE full_name IS NOT NULL ORDER BY merchant
