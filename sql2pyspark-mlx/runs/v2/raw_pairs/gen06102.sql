SELECT status FROM orders WHERE status IN ('SHIPPED', 'CANCELLED', 'PENDING') UNION ALL SELECT status FROM accounts WHERE opened_date >= '2023-11-01';
