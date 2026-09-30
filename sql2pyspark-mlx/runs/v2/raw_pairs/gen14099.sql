SELECT status FROM orders WHERE shipping_fee BETWEEN 35 AND 38 UNION ALL SELECT status FROM accounts WHERE status <> 'OPEN' ORDER BY status DESC
