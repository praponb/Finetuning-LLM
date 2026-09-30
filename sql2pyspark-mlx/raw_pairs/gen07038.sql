SELECT status FROM orders WHERE customer_id IN (12, 34) UNION ALL SELECT status FROM accounts;
