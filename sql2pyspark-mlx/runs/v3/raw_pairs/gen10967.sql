SELECT status, 'account' AS source FROM accounts UNION SELECT status, 'payment' AS source FROM payments WHERE paid_at < '2023-03-01' ORDER BY source;
