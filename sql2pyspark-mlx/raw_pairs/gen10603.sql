SELECT status FROM accounts INTERSECT SELECT city AS status FROM suppliers ORDER BY status DESC;
