SELECT status FROM orders WHERE customer_id IN (9, 20, 26, 27) UNION ALL SELECT status FROM accounts WHERE opened_date >= '2023-10-18'
