SELECT o.store_id, c.country FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount BETWEEN 530 AND 1400 AND o.status = 'RETURNED' ORDER BY country, o.store_id DESC
