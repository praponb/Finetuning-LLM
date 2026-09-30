SELECT txn_type FROM transactions INTERSECT SELECT full_name AS txn_type FROM employees ORDER BY txn_type
